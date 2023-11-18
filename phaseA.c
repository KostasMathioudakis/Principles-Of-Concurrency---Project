/**     cs486
*	@author: Kostas Mathioudakis csd3982
*	tested with max(n) = 213 threads
*	n=180 timings:
*	real    0m9.339s
*	user    0m21.810s
*	sys     0m11.281s
*/

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>
#include "phaseA.h"
#define CAS(A,B,C) __sync_bool_compare_and_swap((long*)A,(long*)B,(long*)C)
#define MIN 5
#define MAX 15


LinkedList * list;
pthread_barrier_t phase_barrier;
HTNode ***hash_tables;
stackNode *stack;

/*--------------------------------------------------------*/
/*-------------------- Misc Functions --------------------*/
/*--------------------------------------------------------*/

DLLNode* DLL_new_node(int id){
	DLLNode * new = (DLLNode *)malloc(sizeof(DLLNode));
	new->productID = id;
	new->next = NULL;
	new->prev = NULL;
	pthread_mutex_init(&new->lock,NULL);
	return new;
}

HTNode* HT_new_node(int id){
	HTNode * new = (HTNode *)malloc(sizeof(HTNode));
	new->productID = id;
	pthread_mutex_init(&new->lock,NULL);
	return new;
}

stackNode* new_stackNode(int id){
	stackNode* new = (stackNode*)malloc(sizeof(stackNode));
	new->productID = id;
	new->next = NULL;
	return new;
}

int get_prime(int key , int arg);
int get_size(int N){
	int size = get_prime(3*N,1);
	return size;
}

int print_list_from_head(DLLNode* head){
		printf("PRINT from head:\n");
		DLLNode* pred = head;
		DLLNode* curr = head->next;
		pthread_mutex_lock(&pred->lock);
		pthread_mutex_lock(&curr->lock);
		while(curr->productID !=-1){
			pthread_mutex_unlock(&pred->lock);
			pred = curr;
			printf("%d ",curr->productID);
			curr = curr->next;
			pthread_mutex_lock(&curr->lock);
		}
		pthread_mutex_unlock(&pred->lock);
		pthread_mutex_unlock(&curr->lock);
		printf("\n");
		return 1;
}

int print_list_from_tail(DLLNode* tail){
		printf("PRINT from tail:\n");
		DLLNode* pred = tail;
		DLLNode* curr = tail->prev;
		//pthread_mutex_lock(&pred->lock);
		//pthread_mutex_lock(&curr->lock);
		while(curr->productID !=-1){
			//pthread_mutex_unlock(&pred->lock);
			pred = curr;
			printf("%d ",curr->productID);
			curr = curr->prev;
			//pthread_mutex_lock(&curr->lock);
		}
		//pthread_mutex_unlock(&pred->lock);
		//pthread_mutex_unlock(&curr->lock);
		printf("\n");
		return 1;
}

int print_tables(int N,int M){
	int i,j,size=get_size(N),keysum;
	for(i=0;i<M;i++){
		for(j=0;j<size;j++){
			if(hash_tables[i][j]!=NULL){
				if(hash_tables[i][j]->productID!=-1)
					printf("[%d]",hash_tables[i][j]->productID);
			}
		}
		printf("\n");
	}
	printf("\n");
	return 1;
}

void print_stack(){
	stackNode *curr = stack;
	printf("Stack: ");
	while(curr!=NULL){
		printf("[%d] ",curr->productID);
		curr = curr->next;
	}
	printf("\n");
	return;
}
/*------------------------------------------------*/
/*-------------------- Innits --------------------*/
/*------------------------------------------------*/

LinkedList * innit_list(){
	LinkedList* new_list = (LinkedList*)malloc(sizeof(LinkedList));
	new_list->head = (DLLNode*)malloc(sizeof(DLLNode));
	new_list->tail = (DLLNode*)malloc(sizeof(DLLNode));
	new_list->head->productID = -1;
	new_list->tail->productID = -1;
	pthread_mutex_init(&new_list->head->lock,NULL);
	pthread_mutex_init(&new_list->tail->lock,NULL);
	new_list->head->prev = NULL;
	new_list->tail->next = NULL;
	new_list->tail->prev = new_list->head;
	new_list->head->next = new_list->tail;
	if (new_list == NULL){
		printf("Failed to innit linked list\n");
		return NULL;
	}
	return new_list;
}

HTNode*** innit_hash_tables(int N){
	int M = N/3, i, k, size = get_size(N);
	HTNode*** new_hash_tables = (HTNode ***)malloc(M*sizeof(HTNode**));
	for(i=0;i<M;i++){
		new_hash_tables[i] = (HTNode**)malloc(size*sizeof(HTNode*));
		for(k=0;k<size;k++){
			new_hash_tables[i][k] = HT_new_node(-1);
		}
	}
	return new_hash_tables;
}



/*--------------------------------------------------------*/
/*-------------------- List Functions --------------------*/
/*--------------------------------------------------------*/

int DLLinsert(int id,DLLNode *head){
	DLLNode *pred,*curr;
	int result;

	pthread_mutex_lock(&head->lock);
	pred = head;
	curr = pred->next;
	pthread_mutex_lock(&curr->lock);
	while(curr->productID < id && curr->productID!=-1){
		pthread_mutex_unlock(&pred->lock);
		pred = curr;
		curr = curr->next;
		pthread_mutex_lock(&curr->lock);
	}
	if(id == curr->productID) result = 0;
	else{
		DLLNode *new = DLL_new_node(id);
		new->next = curr;
		new->prev = pred;
		pred->next = new;
		curr->prev = new;
		result = 1;
	}
	pthread_mutex_unlock(&pred->lock);
	pthread_mutex_unlock(&curr->lock);
	return result;
}

int DLLdelete(int id,DLLNode *head){
	DLLNode *pred, *curr;
	int result;
	pthread_mutex_lock(&head->lock);
	pred = head;
	curr = pred->next;
	pthread_mutex_lock(&curr->lock);
	while(curr->productID<id && curr->productID!=-1){
		pthread_mutex_unlock(&pred->lock);
		pred=curr;
		curr=curr->next;
		pthread_mutex_lock(&curr->lock);
	}
	if(id == curr->productID){
		pred->next = curr->next;
		curr->next->prev = pred;
		result = 1;
	}
	else result = 0;
	pthread_mutex_unlock(&pred->lock);
	pthread_mutex_unlock(&curr->lock);
	return result;
}


int result[2]={0,0};
void list_size_check(DLLNode* head){
	int counter = 0, keysum = 0, exp;
	DLLNode* pred = head;
	DLLNode* curr = head->next;
	pthread_mutex_lock(&pred->lock);
	pthread_mutex_lock(&curr->lock);
	while(curr->productID !=-1){
		pthread_mutex_unlock(&pred->lock);
		pred = curr;
		counter++;
		keysum+=curr->productID;
		curr = curr->next;
		pthread_mutex_lock(&curr->lock);
	}
	pthread_mutex_unlock(&pred->lock);
	pthread_mutex_unlock(&curr->lock);
	result[0] = counter;
	result[1] = keysum;
	return;
}

/*--------------------------------------------------------------*/
/*-------------------- Hash Table Functions --------------------*/
/*--------------------------------------------------------------*/

int HT_size_check(int N,int M,int arg){
	int i,j,size=get_size(N),temp_size=0,exp;
	if(arg==0)exp=3*N;
	else exp=2*N;
	for(i=0;i<M;i++){
		for(j=0;j<size;j++){
			if(hash_tables[i][j]!=NULL){
				if(hash_tables[i][j]->productID!=-1)
					temp_size++;
			}
		}
		printf("HT[%d] size check (expected: %d, found: %d)\n",i,exp,temp_size);
		if(temp_size!=exp){
			printf("Error at table:%d\n",i);
			return 0;
		}
		temp_size =0;
	}
	return 1;
}

int HT_keysum_check(int N,int M){
	int i,j,size=get_size(N),temp_keysum=0,exp=((N*N)*((N*N)-1))/2;
	for(i=0;i<M;i++){
		for(j=0;j<size;j++){
			if(hash_tables[i][j]!=NULL){
				if(hash_tables[i][j]->productID!=-1)
					temp_keysum+=hash_tables[i][j]->productID;
			}
		}

	}
	printf("HT keysum check (expected: %d, found: %d)\n",exp,temp_keysum);
	if(temp_keysum!=exp){
			printf("Error at keysum check!\n");
			return 0;
	}
	return 1;
}


int is_prime(int p){
	int i;
	if(p<=1) return 0;
	if(p<=3) return 1;
	if(p%2 == 0 || p%3 == 0) return 0;
	for(i=5;i*i<=p;i=i+6){
		if(p%i == 0 || p%(i+2)==0)
			return 0;
	}
	return 1;
}

int get_prime(int key,int arg){
	int p,flag;
	if(key<=1)
		return 2;
	p = key;
	flag = 0;
	while(flag == 0){
		if(arg == 0)p--;
		if(arg == 1)p++;
		if(is_prime(p)==1)flag = 1;
	}
	return p;
}

int h2(int key,int size){
	/*int p = get_prime(size,0);
	printf("%d\n",p);
	return (key%3)+1;
	return (key%7)+1;
	return p-(key%p);*/
	return (key+1)%size;
}

int h1(int key,int size){
	return key%(size);
}

int HTinsert(int id,int table_index,int N){
	int k=0,index,M=N/3,size =get_size(N),old;
	index = h1(id,size);
	pthread_mutex_lock(&(hash_tables[table_index][index]->lock));
	if(hash_tables[table_index][index]->productID == -1){
		hash_tables[table_index][index]->productID = id;
		pthread_mutex_unlock(&(hash_tables[table_index][index]->lock));
		return 1;
	}
	else{
		while(1){
			old = index;
			index = h2(index,size);
			if(index<size){
				pthread_mutex_lock(&(hash_tables[table_index][index]->lock));
				pthread_mutex_unlock(&(hash_tables[table_index][old]->lock));
				if(hash_tables[table_index][index]->productID == -1){
					hash_tables[table_index][index]->productID = id;
					pthread_mutex_unlock(&(hash_tables[table_index][index]->lock));
					return 1;
				}
				else{
					k++;
				}
			}
			else {
				printf("A\n");
				pthread_mutex_unlock(&(hash_tables[table_index][old]->lock));
				return 0;
			}
		}
	}
}

int HTdelete(int id,int N){
	int table_index,index,k,temp,size=get_size(N),M=N/3,result;
	for(table_index = 0; table_index < M ; table_index++){
		for(index = 0 ; index < size ; index++){
			if(hash_tables[table_index][index] != NULL){
				pthread_mutex_lock(&(hash_tables[table_index][index]->lock));
				if(hash_tables[table_index][index]->productID==id){
					hash_tables[table_index][index]->productID = -1;
					pthread_mutex_unlock(&(hash_tables[table_index][index]->lock));
					return 1;
				}
				pthread_mutex_unlock(&(hash_tables[table_index][index]->lock));
			}
		}
	}
	return 0;
}

int sell_products(int i,int N,int M){
	int temp = i%M,id,lala;

	for(id=N*i;id<=((N*i)+(N-1));id++){
		if(temp == M)temp=0;
		//printf("thread:%d table_index: %d id: %d \n",i,temp,id);
		lala = HTinsert(id,temp,N);
		if(lala==0){
			printf("\nFAILED HT_INSERT %d (WILL NOT DELETE FROM LIST TOO)\n\n",id);
		}
		else{
			DLLdelete(id,list->head);
		}
		temp++;
	}
	return 1;
}

/*--------------------------------------------------------------------*/
/*-------------------- Unbounded lock free stack ---------------------*/
/*--------------------------------------------------------------------*/

int rand_next(){
	int num =  (rand() % (MAX-MIN+1))+MIN;
	return num;
}

void BackOff(){
	static int limit =  MIN;
	int delay = rand_next();
	if(MAX<2*limit)limit=MAX;
	else limit=2*limit;
	usleep(delay);
}

int TryPush(stackNode* n){
	stackNode *oldTop = stack;
	n->next = oldTop;
	long *a,*b,*c;
	a = (long*) stack;
	b = (long*) oldTop;
	c = (long*) n;
	/*printf("a:%ld b:%ld c:%ld \n",a,b,c);
	  print_stack();*/
	if(CAS(&stack,oldTop,n))
		return 1;
	else return 0;
}

void push(int id){
	stackNode *n = new_stackNode(id);
	while(1){
		if(TryPush(n)==1)return;
		else BackOff(MIN,MAX);
	}
}

stackNode* TryPop(){
	stackNode *oldTop = stack;
	stackNode *newTop;
	if(oldTop==NULL){
		return NULL;
	}
	newTop = oldTop->next;
	if(CAS(&stack,oldTop,newTop))
		return oldTop;
	else return NULL;
}

int pop(){
	stackNode *rn;
	while(1){
		rn = TryPop();
		if(rn==NULL){
			return -1;
		}
		if(rn!=NULL){
			return rn->productID;
		}
		else BackOff();
	}
}

int gather_faulty_products(int i,int N,int M){
        int temp = i%M,lala,id,min=0,max=3*N,index,table_index;

	for(table_index = temp ; table_index < M ; table_index++){
		while(1){
			index = ( rand() % (max-min+1) ) +min;
			if(hash_tables[table_index][index]!=NULL)
				if(hash_tables[table_index][index]->productID != -1){
					id = hash_tables[table_index][index]->productID;
					if(HTdelete(id,N)==1){
						push(id);
						break;
					}
				}
		}
	}
	for(table_index=0;table_index<temp;table_index++){
		while(1){
                        index = ( rand() % (max-min+1) ) +min;
                        if(hash_tables[table_index][index]!=NULL)
                                if(hash_tables[table_index][index]->productID != -1){
                                        id = hash_tables[table_index][index]->productID;
                                        if(HTdelete(id,N)==1){
						push(id);
						break;
					}
                                }
                }
	}
        return 1;
}

int stack_size_check(int N){
	int counter=0,exp=(N*N)/3;
	stackNode *curr = stack;
	while(curr!=NULL){
		if(curr->productID != -1){
			counter++;
		}
		curr = curr->next;
	}
	printf("Stack size check (expected: %d, found: %d)\n",exp,counter);
	if(counter == exp) return 1;
	else return 0;
}

int replace_with_repaired(){
	int id;
	while(1){
		id = pop();
		if(id!=-1)
			DLLinsert(id,list->head);
		else
			break;
	}
	return 1;
}

/*--------------------------------------------------------------------*/
/*-------------------- Thread Function -------------------------------*/
/*--------------------------------------------------------------------*/

int flag = 1;
void* thread_func(void* _arg){
	argument* arg = (argument*)_arg;
	int j=arg->j , N=arg->N , i , newID , size_exp = N*N , keysum_exp = ( (N*N) * ((N*N)-1) )/2, M = N/3,checks=1;


	/*--------------------------------------------------------------*/
	/*------------------- 1st Phase --------------------------------*/
	/*--------------------------------------------------------------*/

	for(i=0;i<=N-1;i++){
		newID = i*N+j;
		DLLinsert(newID,list->head);
	}
	pthread_barrier_wait(&phase_barrier);
	if(j==0){
		list_size_check(list->head);
		printf("List size check   (expected: %d, found: %d)\n",size_exp,result[0]);
		printf("List keysum check (expected: %d, found: %d)\n",keysum_exp,result[1]);
		if(result[0]!=size_exp){
			printf("List Size Check returned ERROR !\n");
			exit(0);
		}
		if(result[1]!=keysum_exp){
			printf("Keysum Check returned ERROR !\n");
			exit(0);
		}
	}
	pthread_barrier_wait(&phase_barrier);

	/*--------------------------------------------------------------*/
	/*------------------- 2nd Phase --------------------------------*/
	/*--------------------------------------------------------------*/

	sell_products(j,N,M);
	pthread_barrier_wait(&phase_barrier);
	if(j==0){
		checks*=HT_size_check(N,M,0);
		checks*=HT_keysum_check(N,M);
		if(checks==0){
			printf("HT Checks Failed Exiting...\n");
			exit(0);
		}
	}
	pthread_barrier_wait(&phase_barrier);

	/*--------------------------------------------------------------*/
	/*------------------- 3rd Phase --------------------------------*/
	/*--------------------------------------------------------------*/

	gather_faulty_products(j,N,M);
	pthread_barrier_wait(&phase_barrier);
	if(j==0){
		checks = 1;
		checks*=HT_size_check(N,M,1);
		checks*=stack_size_check(N);
		if(checks==0){
			printf("Stack size checks Failed Exiting...\n");
			exit(0);
		}
	}
	pthread_barrier_wait(&phase_barrier);
        /*--------------------------------------------------------------*/
        /*------------------- 4th Phase --------------------------------*/
        /*--------------------------------------------------------------*/
	replace_with_repaired();
	pthread_barrier_wait(&phase_barrier);
	if(j==0){
		size_exp = (N*N)/3;
		list_size_check(list->head);
		printf("List size check (expected: %d, found: %d)\n",size_exp,result[0]);
		if(result[0]!=size_exp){
			printf("List size check returned ERROR !\n");
			exit(0);
		}
	}
	return NULL;
}


int main(int argc,char** argv){

	/*---------------------------	Initializing	----------------------------------*/
	srand(time(0));
	list = innit_list();

	int j,N = atoi(argv[1]),M;
	if( (N%3) != 0 ){
		printf("Invalid Argument!\n");
		printf("Argument must be multiple of  3 (3,6,9,12..etc) !\n");
		printf("Rerun with a correct argument !\n");
		return 0;
	}
	if(list==NULL){
		printf("Error in innit_phaseA\n");
		return 0;
	}

	M=N/3;
	hash_tables = innit_hash_tables(N);
	if(hash_tables==NULL){
		printf("Error in initializing hash tables (1) \n");
		return 0;
	}
	for(j=0;j<M;j++){
		if(hash_tables[j] == NULL ){
			printf("Error at initializing hash tables (2) \n");
			return 0;
		}
	}
        /*----------------------        Thread Creation and Join    -----------------------*/
	pthread_t threads[N];
	pthread_barrier_init(&phase_barrier,NULL,N);
	for (j=0;j<N;j++){
		argument * arg = (argument*)malloc(sizeof(argument));
		arg->j = j;
		arg->N = N;
		if(pthread_create( &(threads[j]), NULL, &thread_func, (void*)arg)!=0){
			printf("Error at creating thread:%d\n",j);
			return 0;
		}
	}
	for(j=0;j<N;j++){
		pthread_join(threads[j],NULL);
	}
	pthread_barrier_destroy(&phase_barrier);

	return 1;
}
