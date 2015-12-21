#include "common.h"
#include <pthread.h>

#define ITEMS_PER_ALLOC 64
static LIBEVENT_DISPATCHER_THREAD dispatcher_thread;

/*
 * Each libevent instance has a wakeup pipe, which other threads
 * can use to signal that they've put a new connection on its queue.
 */
static LIBEVENT_THREAD *threads;

static int init_count = 0;
static pthread_mutex_t init_lock;
static pthread_cond_t init_cond;

/* Free list of CQ_ITEM structs */
static CQ_ITEM *cqi_freelist;
static pthread_mutex_t cqi_freelist_lock;

static pthread_mutex_t *item_locks;
/* size of the item lock hash table */
static uint32_t item_lock_count;
unsigned int item_lock_hashpower;
#define hashsize(n) ((unsigned long int)1<<(n))
#define hashmask(n) (hashsize(n)-1)

/* Which thread we assigned a connection to most recently. */
static int last_thread = -1;

pthread_mutex_t atomics_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Initializes a connection queue.
 */
static void cq_init(CQ *cq) 
{
    pthread_mutex_init(&cq->lock, NULL);
    cq->head = NULL;
    cq->tail = NULL;
}

/*
 * Looks for an item on a connection queue, but doesn't block if there isn't
 * one.
 * Returns the item, or NULL if no item is available
 */
static CQ_ITEM *cq_pop(CQ *cq) 
{
    CQ_ITEM *item;

    pthread_mutex_lock(&cq->lock);
    item = cq->head;
    if (NULL != item) {
        cq->head = item->next;
        if (NULL == cq->head)
            cq->tail = NULL;
    }
    pthread_mutex_unlock(&cq->lock);

    return item;
}

/*
 * Adds an item to a connection queue.
 */
static void cq_push(CQ *cq, CQ_ITEM *item) 
{
    item->next = NULL;

    pthread_mutex_lock(&cq->lock);
    if (NULL == cq->tail)
        cq->head = item;
    else
        cq->tail->next = item;
    cq->tail = item;
    pthread_mutex_unlock(&cq->lock);
}

/*
 * Frees a connection queue item (adds it to the freelist.)
 */
static void cqi_free(CQ_ITEM *item) 
{
    pthread_mutex_lock(&cqi_freelist_lock);
    item->next = cqi_freelist;
    cqi_freelist = item;
    pthread_mutex_unlock(&cqi_freelist_lock);
}

static CQ_ITEM *cqi_new(void)
{
	CQ_ITEM *item = NULL;
	pthread_mutex_lock(&cqi_freelist_lock);
	if(cqi_freelist)
	{
		item = cqi_freelist;
		cqi_freelist = item->next;
	}
	pthread_mutex_unlock(&cqi_freelist_lock);	

	if(NULL == item)
	{
		int i;
	
		item = malloc(sizeof(CQ_ITEM) * ITEMS_PER_ALLOC);
		if(NULL == item)
		{
			perror("malloc");
			return NULL;
		}

		/*
         * Link together all the new items except the first one
         * (which we'll return to the caller) for placement on
         * the freelist.
         */
        for (i = 2; i < ITEMS_PER_ALLOC; i++)
            item[i - 1].next = &item[i];

        pthread_mutex_lock(&cqi_freelist_lock);
        item[ITEMS_PER_ALLOC - 1].next = cqi_freelist;
        cqi_freelist = &item[1];
        pthread_mutex_unlock(&cqi_freelist_lock);
	}

	return item;
}

unsigned short refcount_decr(unsigned short *refcount) {
#ifdef HAVE_GCC_ATOMICS
    return __sync_sub_and_fetch(refcount, 1);
#elif defined(__sun)
    return atomic_dec_ushort_nv(refcount);
#else
    unsigned short res;
    pthread_mutex_lock(&atomics_mutex);
    (*refcount)--;
    res = *refcount;
    pthread_mutex_unlock(&atomics_mutex);
    return res;
#endif
}

void do_item_remove(item *it) {
    assert((it->it_flags & ITEM_SLABBED) == 0);
    assert(it->refcount > 0);

    if (refcount_decr(&it->refcount) == 0) { 
        //item_free(it);
		printf("1111111\n");
    }    
}

/*      
 * Decrements the reference count on an item and adds it to the freelist if
 * needed.
 */
void item_remove(item *item) {
    do_item_remove(item);
}

/*
 * Dispatches a new connection to another thread. This is only ever called
 * from the main thread, either during initialization (for UDP) or because
 * of an incoming connection.
 */
void dispatch_conn_new(int sfd, int init_state, int event_flags, int read_buffer_size)
{
	CQ_ITEM *item = cqi_new();
	char buf[1];
	if(item == NULL)
	{
		close(sfd);
		fprintf(stderr, "Failed to allocate memory for connection object\n");
        return ;
	}
	
	int tid = (last_thread + 1) % 4;
	LIBEVENT_THREAD *thread = threads + tid;
	last_thread = tid;

	item->sfd = sfd;
	item->init_state = init_state;
	item->event_flags = event_flags;
	item->read_buffer_size = read_buffer_size;
	
	cq_push(thread->new_conn_queue, item);	
	buf[0] = 'c';
	if(write(thread->notify_send_fd, buf, 1) != 1)
	{
		perror("Writing to thread notify pipe");
	}
}

/*
 * Processes an incoming "handle a new connection" item. This is called when
 * input arrives on the libevent wakeup pipe.
 */
static void thread_libevent_process(int fd, short which, void *arg)
{
	LIBEVENT_THREAD *me = arg;
    int n;
	char buf[1];
	char buff[100];
	CQ_ITEM *item;

	if (read(fd, buf, 1) != 1)
	{
        fprintf(stderr, "Can't read from libevent pipe\n");
	}

	item = cq_pop(me->new_conn_queue);

	if(NULL != item)
	{
		conn *c = conn_new(item->sfd, item->init_state, item->event_flags, item->read_buffer_size, me->base);
		if(c == NULL)
		{
			fprintf(stderr, "Can't listen for events on socket on fd %d\n", item->sfd);
			close(item->sfd);
			
		}
		else
		{
			c->thread = me;
		}
		cqi_free(item);
	}
}

static void setup_thread(LIBEVENT_THREAD *me)
{
	me->base = event_base_new();
    if (!me->base) 
	{
        fprintf(stderr, "Can't allocate event base\n");
        exit(1);
    }

	/* Listen for notifications from other threads */
	event_set(&me->notify_event, me->notify_receive_fd, EV_READ | EV_PERSIST, thread_libevent_process, me);
	event_base_set(me->base, &me->notify_event);

    if (event_add(&me->notify_event, 0) == -1) 
	{
        fprintf(stderr, "Can't monitor libevent notify pipe\n");
        exit(1);
    }

	me->new_conn_queue = malloc(sizeof(struct conn_queue));
    if (me->new_conn_queue == NULL) 
	{
        perror("Failed to allocate memory for connection queue");
        exit(EXIT_FAILURE);
    }
    cq_init(me->new_conn_queue);

    if (pthread_mutex_init(&me->stats.mutex, NULL) != 0) 
	{
        perror("Failed to initialize mutex");
        exit(EXIT_FAILURE);
    }
}

static void wait_for_thread_registration(int nthreads)
{
	while(init_count < nthreads)
	{
		pthread_cond_wait(&init_cond, &init_lock);
	}
}

static void register_thread_initialized(void)
{
	pthread_mutex_lock(&init_lock);
	init_count++;
	pthread_cond_signal(&init_cond);
	pthread_mutex_unlock(&init_lock);
}

/*
 * Worker thread: main event loop
 */
static void *worker_libevent(void *arg) 
{
    LIBEVENT_THREAD *me = arg;

    /* Any per-thread setup can happen here; memcached_thread_init() will block until
     * all threads have finished initializing.
     */
	register_thread_initialized();

	event_base_dispatch(me->base);
    return NULL;
}

/*
 * Creates a worker thread.
 */
static void create_worker(void *(*func)(void *), void *arg) 
{
    pthread_t       thread;
    pthread_attr_t  attr;
    int             ret;

    pthread_attr_init(&attr);

    if ((ret = pthread_create(&thread, &attr, func, arg)) != 0) 
	{
        fprintf(stderr, "Can't create thread: %s\n",
                strerror(ret));
        exit(1);
    }
}

void memcached_thread_init(int nthreads, struct event_base *main_base)
{
	int i;

	pthread_mutex_init(&init_lock, NULL);
	pthread_cond_init(&init_cond, NULL);

	threads = calloc(nthreads, sizeof(LIBEVENT_THREAD));
    if (!threads) 
	{
        perror("Can't allocate thread descriptors");
        exit(1);
    }

	dispatcher_thread.base = main_base;
	dispatcher_thread.thread_id = pthread_self();
	
	for(i = 0; i < nthreads; i++)
	{
		int fds[2];
		if(pipe(fds))
		{
			perror("Can't create notify pipe");
            exit(1);
		}

		threads[i].notify_receive_fd = fds[0];
		threads[i].notify_send_fd = fds[1];
		setup_thread(&threads[i]);
	}	

	/* Create threads after we've done all the libevent setup. */
	for(i = 0; i < nthreads; i++)
	{
		create_worker(worker_libevent, &threads[i]);
	}

	pthread_mutex_lock(&init_lock);
    wait_for_thread_registration(nthreads);
    pthread_mutex_unlock(&init_lock);
}
