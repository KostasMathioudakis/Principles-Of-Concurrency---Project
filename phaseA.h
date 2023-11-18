#include <pthread.h>

typedef struct DLLNode{
	int productID;
	pthread_mutex_t lock;
	struct DLLNode *next;
	struct DLLNode *prev;
}DLLNode;

typedef struct LinkedList{
	struct DLLNode *head;
	struct DLLNode *tail;
}LinkedList;

typedef struct HTNode{
	int productID;
	pthread_mutex_t lock;
}HTNode;

typedef struct stackNode{
	int productID;
	struct stackNode *next;
}stackNode;

typedef struct argument{
	int N;
	int j;
}argument;

