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

#ifndef NELEMENTS
#define NELEMENTS 100
#endif

#ifndef MAXVAL
#define MAXVAL 255
#endif // MAX_VAL

#ifndef PRINT 
#define PRINT 1
#endif

// Funcoes de inicializacao e verificacao de variaveis
void initializeVariables(int argc, char ** argv, int* seed, int* max_val, int* print, size_t *arr_size);
void populate_array(int* array, int size, int max, int seed);
void print_array(int* array, int size);

// Funcoes quanto a parte de merge sort do programa (adequado tanto para
// 1 processo quanto para varios, com partes que recebem tanto 1 array
// como 2 arrays)
void recursive_merge_sort(int* tmp, int begin, int end, int* numbers);
void mergeWithTwo(int* numbersLeft, int* numbersRight, int sizeLeft, int sizeRight, int * sorted);
void mergeWithOne(int* numbers, int begin, int middle, int end, int * sorted);
void merge_sortWithTwo(int *arrayLeft, int sizeLeft, int *arrayRight, int sizeRight, int *tmp);
void merge_sortWithOne(int* numbers, int size, int * tmp);
void recursive_merge_sort(int* tmp, int begin, int end, int* numbers);


// Funcoes para comunicacao de processos e funcoes auxiliares a estas
void receiveInfos();
void sortBack(int *rightArray, int rightSize);
void recursiveDivideArrayReceived(int* arrayReceived, int arraySize);
int** divideArray(int* numbers, int size);
void receiveArrayToDivide();
void sendInfosToProcess(int *rightArray, int rightSize, int dest);
void excluirProcessosDesnecessarios(int maxProc);
int informaNumeroMaxDeProcessosNecessarios(int argc, char** argv);

// variaveis  para controle do ambiente MPI
int quant_processes, rank;

// Comunicador usado neste ambiente
MPI_Comm myCOMM;

// step indica o nivel atual que a arvore de processos se encontra
// activated indica ao processo se ele esta realizando divisao de um vetor
// Sequential indica se só existe um processo (realiza sequencial)
int step, sequential;

int main (int argc, char ** argv) {
	int seed, max_val, print;
	int * sortable;
	int * tmp;
	size_t arr_size;

	// Seta variaveis de controle do nivel da arvore e signal ativado
	// para todos os processos (pois nivel = 0 e nenhuma esta ativada)
	if (rank == 0)
		step = 0;

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
		populate_array(sortable, arr_size, max_val, seed);
		memcpy(tmp, sortable, arr_size*sizeof(int));

		if (print)
			print_array(sortable, arr_size);

		if ((!sequential) && (quant_processes > 1)) {
			recursiveDivideArrayReceived(sortable, arr_size);
		} else {
			merge_sortWithOne(sortable, arr_size, tmp);
			if (print)
				print_array(tmp, arr_size);

			free(sortable);
			free(tmp);
		}
	} else {
		if (rank > 0)
			receiveArrayToDivide();
	}

	MPI_Finalize();
	return 0;
}

/*
 * Orderly merges two int arrays (numbers[begin..middle] and numbers[middle..end]) into one (sorted).
 * \retval: merged array -> sorted
 */
void mergeWithTwo(int* numbersLeft, int *numbersRight, int sizeLeft, int sizeRight, int * sorted) {
	int i, j;
	i = 0; j = 0;
	for (int k = 0; k < sizeLeft + sizeRight; ++k) {
		if (i < sizeLeft && (j >= sizeRight || numbersLeft[i] < numbersRight[j])) {
			sorted[k] = numbersLeft[i];
			i++;
		} else {
			sorted[k] = numbersRight[j];
			j++;
		}
	}
}

void mergeWithOne(int* numbers, int begin, int middle, int end, int * sorted) {
	int i, j;
	i = begin; j = middle;
	for (int k = begin; k < end; ++k) {
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
		mergeWithOne(tmp, begin, middle, end, numbers);
	}
}

// First Merge Sort call (Called from process 0)
void merge_sortWithTwo(int *arrayLeft, int sizeLeft, int *arrayRight, int sizeRight, int *tmp) {
	// In this function the pararel region is defined
	int *tmpAux1, *tmpAux2;
	if (sizeLeft > 1) {
		tmpAux1 = malloc(sizeLeft * sizeof(int));
		memcpy(tmpAux1, arrayLeft, sizeLeft * sizeof(int));
		recursive_merge_sort(tmpAux1, 0, sizeLeft, arrayLeft);
		free(tmpAux1);
	}
	if (sizeRight > 1) {
		tmpAux2 = malloc(sizeRight * sizeof(int));
		memcpy(tmpAux2, arrayRight, sizeRight * sizeof(int));
		recursive_merge_sort(tmpAux2, 0, sizeRight, arrayRight);
		free(tmpAux2);
	}
	mergeWithTwo(arrayLeft, arrayRight, sizeLeft, sizeRight, tmp);
}

void merge_sortWithOne(int* numbers, int size, int * tmp) {
	// In this function the pararel region is defined
	recursive_merge_sort(numbers, 0, size, tmp);
}

void sendInfosToProcess(int *rightArray, int rightSize, int dest) {
	MPI_Send(&step, 1, MPI_INT, dest, 0, myCOMM);
	MPI_Send(&rightSize, 1, MPI_INT, dest, 1, myCOMM); 
	MPI_Send(rightArray, rightSize, MPI_INT, dest, 2, myCOMM);
}

void receiveArrayToDivide() {
	MPI_Recv(&step, 1, MPI_INT, MPI_ANY_SOURCE, 0, myCOMM, NULL);
	if (step != -1) {
		int arraySize;
		MPI_Recv(&arraySize, 1, MPI_INT, MPI_ANY_SOURCE, 1, myCOMM, NULL);

		int* arrayRecv = (int *) malloc(arraySize * sizeof(int));

		MPI_Recv(arrayRecv, arraySize, MPI_INT, MPI_ANY_SOURCE, 2, myCOMM, NULL);

		recursiveDivideArrayReceived(arrayRecv, arraySize);
	} else {
		printf("Rank %d nao necessario \n", rank);
	}
}

void recursiveDivideArrayReceived(int* arrayReceived, int arraySize) {
	// Calcular qual processo deve receber o array direito para dividir
	// Calculo baseado nos "steps" de uma arvore
	int dest = rank + pow(2, step);
	step++;

	int **resp = divideArray(arrayReceived, arraySize);
	// Verifica se existe o destino calculado e se o tamanho do array
	// a ser enviado a ele é relavante (>1), se 
	if ((arraySize > 3) && (dest < quant_processes)) {

		sendInfosToProcess(resp[3], *resp[2], dest);
		recursiveDivideArrayReceived(resp[1], *resp[0]);
	} else {
		if (dest < quant_processes) {
			int sendInfo = -1;
			MPI_Send(&sendInfo, 1, MPI_INT, dest, 0, myCOMM);
		}

		int* tmp = malloc(arraySize * sizeof(int));
		merge_sortWithTwo(resp[1], *resp[0], resp[3], *resp[2], tmp);
		print_array(tmp, arraySize);
		sortBack(tmp, arraySize);
	}
}

void sortBack(int *arrayLeft, int sizeLeft) {
	//int rankSource = 


	//
	int sizeRight;
	//
	int *arrayRight;

	int *tmp = malloc((sizeLeft + sizeRight) * sizeof(int));
	mergeWithTwo(arrayLeft, sizeLeft, arrayRight, sizeRight, tmp);

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
	// definicao esta errada
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

void populate_array(int* array, int size, int max, int seed) {
	int m = max+1;
	srand(seed);
	for (int i = 0; i < size; ++i) {
		array[i] = rand()%m;
	}
}

int informaNumeroMaxDeProcessosNecessarios(int argc, char** argv){
	int nElem;
	if (argc < 3)
		nElem = NELEMENTS;
	else 
		nElem = (atoi(argv[2]));

	int cont = 0;

	return nElem/2;
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
