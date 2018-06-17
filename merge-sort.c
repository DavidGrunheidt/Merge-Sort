// V. Freitas [2018] @ ECL-UFSC
#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <stdarg.h>
#include <mpi.h>

/*** 
 * Todas as Macros pré-definidas devem ser recebidas como parâmetros de
 * execução da sua implementação paralela!! 
 ***/
#ifndef DEBUG
#define DEBUG 0
#endif

#ifndef NELEMENTS
#define NELEMENTS 100
#endif

#ifndef MAXVAL
#define MAXVAL 255
#endif // MAX_VAL

#ifndef PRINT 
#define PRINT 1
#endif

void debug(const char* msg, ...);
int test();

void initializeVariables(int argc, char ** argv, int* seed, int* max_val, int* print, size_t *arr_size);
void populate_array(int* array, int size, int max);
void print_array(int* array, int size);
void recursive_merge_sort(int* tmp, int begin, int end, int* numbers);
void merge(int* numbers, int begin, int middle, int end, int * sorted);
void merge_sort(int* numbers, int size, int * tmp);
void recursive_merge_sort(int* tmp, int begin, int end, int* numbers);

void receiveArrayToDivide();
void recursiveDivideArrayReceived(int* arrayReceived, int arraySize);
int** divideArray(int* numbers, int size);
void sendInfosToProcess(int *rightArray, int rightSize, int dest);
void  comunicarStep();
void letsSortThisThing (int* rightArray, int rightSize, int* leftArray, int leftSize);
void excluirProcessosDesnecessarios(int maxProc);
int informaNumeroMaxDeProcessosNecessarios(int argc, char** argv);

// variaveis  para controle do ambiente MPI
int quant_processes, rank;

// Comunicador usado neste ambiente
MPI_Comm myCOMM;

// step indica o nivel atual que a arvore de processos se encontra
// activated indica ao processo se ele esta realizando divisao de um vetor
// Sequential indica se só existe um processo (realiza sequencial)
int step, activated, sequential;

int main (int argc, char ** argv) {
	if (DEBUG > 0)
		return test();

	int seed, max_val, print;
	int * sortable;
	int * tmp;
	size_t arr_size;

	// Seta variaveis de controle do nivel da arvore e signal ativado
	// para todos os processos (pois nivel = 0 e nenhuma esta ativada)
	step = 0;
	activated = 0;

	/////////////////// Inicio da regiao paralela MPI//////////////////
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &quant_processes);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	//Verifica quantos processos necessarios baseado no tamanho do array
	int maxProc = informaNumeroMaxDeProcessosNecessarios(argc, argv);
	excluirProcessosDesnecessarios(maxProc);

	// Processo 0 responasvel por "popular o vetor inicial nulo"
	if (rank == 0) {
		// Inicialização de variavies segundo parametros passados por linha de comando
		initializeVariables(argc, argv, &seed, &max_val, &print, &arr_size);

		// Alocacao de memoria
		sortable = malloc(arr_size*sizeof(int));
		tmp 	 = malloc(arr_size*sizeof(int));

		// Inserção de valores no array original
		populate_array(sortable, arr_size, max_val);
		memcpy(tmp, sortable, arr_size*sizeof(int));

		if (print)
			print_array(sortable, arr_size);

		if ((!sequential) && (quant_processes > 1)) {
			recursiveDivideArrayReceived(sortable, arr_size);
		} else {
			merge_sort(sortable, arr_size, tmp);
			if (print)
				print_array(tmp, arr_size);

			free(sortable);
			free(tmp);
		}
	} else {
		if (rank > 0)
			comunicarStep();
	}

	MPI_Finalize();
	return 0;
}

/*
 * Orderly merges two int arrays (numbers[begin..middle] and numbers[middle..end]) into one (sorted).
 * \retval: merged array -> sorted
 */
void merge(int* numbers, int begin, int middle, int end, int * sorted) {
	int i, j;
	i = begin; j = middle;
	debug("Merging. Begin: %d, Middle: %d, End: %d\n", begin, middle, end);
	for (int k = begin; k < end; ++k) {
		debug("LHS[%d]: %d, RHS[%d]: %d\n", i, numbers[i], j, numbers[j]);
		if (i < middle && (j >= end || numbers[i] < numbers[j])) {
			sorted[k] = numbers[i];
			i++;
		} else {
			sorted[k] = numbers[j];
			j++;
		}
	}
}


/*
 * Merge sort recursive step adapted for a concurrent context 
 */
void recursive_merge_sort(int* tmp, int begin, int end, int* numbers) {
	if (end - begin < 2)
		return;
	else {
		int middle = (begin + end)/2;
		recursive_merge_sort(numbers, begin, middle, tmp);
		recursive_merge_sort(numbers, middle, end, tmp);
		merge(tmp, begin, middle, end, numbers);
	}
}

// First Merge Sort call (Called from process 0)
void merge_sort(int* numbers, int size, int * tmp) {
	// In this function the pararel region is defined
	recursive_merge_sort(numbers, 0, size, tmp);
}

// Todos os processo necessitam saber o nivel atual da arvore
// pois sera feita verificação a fim de descobrir se aquele processo
// pode ser ativado naquele nivel da arvore (e receber msgs, etc..)
void  comunicarStep(){
	MPI_Bcast(&step, 1, MPI_INT, 0, myCOMM);
	if (!activated) {
		if (rank < pow(2, step)) {
			receiveArrayToDivide();
		} else {
			comunicarStep();
		}
	}
	// Processos que ja estao ativados devem retornar ao metodo
	// ja sendo executado (recursiveDivideArrayReceived)
}	

void sendInfosToProcess(int *rightArray, int rightSize, int dest) {
	MPI_Send(&rightSize, 1, MPI_INT, dest, 0, myCOMM); 
	MPI_Send(rightArray, rightSize, MPI_INT, dest, 1, myCOMM);
}

void receiveArrayToDivide() {
	// é necessario calcular qual o source de um rank no step atual
	int source = rank - pow(2, (step-1)), arraySize;

	MPI_Recv(&arraySize, 1, MPI_INT, source, 0, myCOMM, NULL);

	int* arrayRecv = (int *) malloc(arraySize * sizeof(int));

	MPI_Recv(arrayRecv, arraySize, MPI_INT, source, 1, myCOMM, NULL);

	recursiveDivideArrayReceived(arrayRecv, arraySize);
}

void recursiveDivideArrayReceived(int* arrayReceived, int arraySize) {
	activated = 1;

	// Calcular qual processo deve receber o array direito para dividir
	// Calculo baseado nos "steps" de uma arvore
	int dest = rank + pow(2, step);

	step++;
	comunicarStep();

	// Verifica se existe o destino calculado e se o tamanho do array
	// a ser enviado a ele é relavante (>1), se 
	if ((arraySize > 3) && (dest < quant_processes)) {
		int **resp = divideArray(arrayReceived, arraySize);

		sendInfosToProcess(resp[3], *resp[2], dest);
		recursiveDivideArrayReceived(resp[1], *resp[0]);
	} else {
		int* tmp = malloc(arraySize * sizeof(int));
		memcpy(tmp, arrayReceived, arraySize * sizeof(int));

		merge_sort(arrayReceived, arraySize, tmp); 

		print_array(tmp, arraySize);
		MPI_Barrier(myCOMM);
	}	

}

void letsSortThisThing (int* rightArray, int rightSize, int* leftArray, int leftSize) {

}


void receiveArrayToSort() {

}

// Func retorna diversas informacoes em um ponteiro de ponteiros:
// 1 Elemento = tamanho desta parte esquerda
// 2 Elemento = parte esquerda da divisao do vetor original em 2
// 3 Elemento = tamanho desta parte direita
// 4 Elemento = parte direita da divisao do vetor original em 2
int** divideArray(int* numbers, int size) {
	int **leftRight = (int **) malloc (4 * sizeof(int*));

	// Ponteiros indicando o tamanho de cada vetor após divisao
	int* leftSize = (int *) malloc(sizeof(int));
	int* rightSize = (int *) malloc(sizeof(int));

	// Definicao de valores para cada size
	if (size % 2 == 0)
		*leftSize = ((int)(size/2));
	else
		*leftSize = ((int)(size/2))+1;

	*rightSize = (size - *leftSize);

	// Vetores de valores após divisao em 2 partes
	int* left = (int *) malloc(*leftSize * sizeof(int));
	int* right = (int *) malloc(*rightSize * sizeof(int));

	// Colocando os valores do vetor original nos vetores divididos
	memcpy(left, numbers, *leftSize *sizeof(int));
	memcpy(right, &numbers[*leftSize], *rightSize *sizeof(int));

	// Setando valores de retorno da funcao
	leftRight[0] = leftSize;
	leftRight[1] = left;
	leftRight[2] = rightSize;
	leftRight[3] = right;

	return leftRight;
}

void print_array(int* array, int size) {
	printf("Array do rank %d = [ ", rank);
	for (int i = 0; i < size; i++) 
		printf("%d ", array[i]);
	printf("]\n");
}

void populate_array(int* array, int size, int max) {
	int m = max+1;
	for (int i = 0; i < size; ++i) {
		array[i] = rand()%m;
	}
}

int informaNumeroMaxDeProcessosNecessarios(int argc, char** argv){
	int maxProc;
	if (argc < 3) {
		maxProc = (int) ((int) NELEMENTS)/2;
	} else {
		maxProc = (int) (atoi(argv[2])/2);
	}
	return maxProc;
}

void excluirProcessosDesnecessarios(int maxProc) {
	// Obter o grupo de processos no comunicador global
	MPI_Group world_group;
	MPI_Comm_group(MPI_COMM_WORLD, &world_group);
	if (quant_processes > maxProc) {
		// Criar um novo grupo só de processos necessarios
		MPI_Group new_group;
		int ranges[][3] = {maxProc, quant_processes-1, 1};
		MPI_Group_range_excl(world_group, 1, ranges, &new_group);

		quant_processes = maxProc;

		// Criar um novo comunicador para o grupo de processos necessarios
		MPI_Comm_create(MPI_COMM_WORLD, new_group, &myCOMM);

		if (myCOMM == MPI_COMM_NULL) {
			if (rank > 0) {
				printf("Rank %d finalizado \n", rank);
				fflush(stdout);
				MPI_Finalize();
				exit(0);
			} else {
				sequential = 1;
			}
		}
	} else {
		MPI_Comm_create(MPI_COMM_WORLD, world_group,&myCOMM);
	}
}

void initializeVariables(int argc, char ** argv, int* seed, int* max_val, int* print, size_t *arr_size) {
	switch (argc) {
		case 1:
		*seed = time(NULL);
		*arr_size = NELEMENTS;
		*max_val = MAXVAL;
		*print = PRINT;
		break;
		case 2:
		*seed = atoi(argv[1]);
		*arr_size = NELEMENTS;
		*max_val = MAXVAL;
		*print = PRINT;
		break;
		case 3:
		*seed = atoi(argv[1]);
		*arr_size = atoi(argv[2]);
		*max_val = MAXVAL;
		*print = PRINT;
		break;
		case 4:
		*seed = atoi(argv[1]);
		*arr_size = atoi(argv[2]);
		*max_val = atoi(argv[3]);
		*print = PRINT;
		break;
		case 5:
		*seed = atoi(argv[1]);
		*arr_size = atoi(argv[2]);
		*max_val = atoi(argv[3]);
		*print = atoi(argv[4]);
		break;
		default:
		printf("Too many arguments\n");
		exit(1);	
	}
}

int test() {
		// Basic MERGE unit test
	if (DEBUG > 1) {
		int * a = (int*)malloc(8*sizeof(int));
		a[0] = 1; a[1] = 3; a[2] = 4; a[3] = 7;
		a[4] = 0; a[5] = 2; a[6] = 5; a[7] = 6;
		int * values = (int*)malloc(8*sizeof(int));
		merge(a, 0, 4, 8, values);
		free (a);
		print_array(values, 8);
		free(values);
		return 2;
	}

	// Basic MERGE-SORT unit test
	if (DEBUG > 0) {
		int * a = (int*)malloc(8*sizeof(int));
		int * b = (int*)malloc(8*sizeof(int));
		a[0] = 7; a[1] = 6; a[2] = 5; a[3] = 4;
		a[4] = 3; a[5] = 2; a[6] = 1; a[7] = 0;

		b = memcpy(b, a, 8*sizeof(int));
		merge_sort(a, 8, b);
		print_array(b, 8);

		free(a);
		free(b);

		a = (int*)malloc(9*sizeof(int));
		b = (int*)malloc(9*sizeof(int));
		a[0] = 3; a[1] = 2; a[2] = 1; 
		a[3] = 10; a[4] = 11; a[5] = 12; 
		a[6] = 0; a[7] = 1; a[8] = 1;

		b = memcpy(b, a, 9*sizeof(int));
		print_array(b, 9);

		merge_sort(a, 9, b);
		print_array(b, 9);

		free(a);
		free(b);
		printf("\n");
		return 1;
	}
}

/*
 * More info on: http://en.cppreference.com/w/c/language/variadic
 */
void debug(const char* msg, ...) {
	if (DEBUG > 2) {
		va_list args;
		va_start(args, msg); 
		vprintf(msg, args); 
		va_end(args);
	}
}
