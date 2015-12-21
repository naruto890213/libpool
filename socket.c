#include "common.h"

static pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;
void event_handler(const int fd, const short which, void *arg);

enum transmit_result {
    TRANSMIT_COMPLETE,   /** All done writing. */
    TRANSMIT_INCOMPLETE, /** More data remaining to write. */
    TRANSMIT_SOFT_ERROR, /** Can't write any more right now. */
    TRANSMIT_HARD_ERROR  /** Can't write (c->state is set to conn_closing) */
};

#define IOV_MAX 1024
static int reqs_per_event = 20;//add by 20151126
conn **conns;

static void conn_close(conn *c)
{
	assert(c != NULL);

	event_del(&c->event);
	close(c->sfd);//add by 20151217
	
	return;
}

static int add_msghdr(conn *c)
{
	struct msghdr *msg;
	assert(c != NULL);

	if(c->msgsize == c->msgused)
	{
		msg = realloc(c->msglist, c->msgsize * 2 * sizeof(struct msghdr));
		if(!msg)
		{
			return -1;
		}
		c->msglist = msg;
		c->msgsize *= 2;
	}
	
	msg = c->msglist + c->msgused;
	memset(msg, 0, sizeof(struct msghdr));
	msg->msg_iov = &c->iov[c->iovused];
	c->msgbytes = 0;
	c->msgused++;

	return 0;
}

static int ensure_iov_space(conn *c)
{
	assert(c != NULL);

	if(c->iovused >= c->iovsize)
	{
		int i, iovnum;
		struct iovec *new_iov = (struct iovec *)realloc(c->iov, (c->iovsize * 2) * sizeof(struct iovec));

		if(!new_iov)
		{
			perror("realloc");
			return -1;
		}
		c->iov = new_iov;
		c->iovsize *= 2;

		for(i = 0, iovnum = 0; i < c->msgused; i++)
		{
			c->msglist[i].msg_iov = &c->iov[iovnum];
			iovnum += c->msglist[i].msg_iovlen;
		}
	}

	return 0;
}

static int add_iov(conn *c, const void *buf, int len)
{
	struct msghdr *m;
	int leftover;
	int limit_to_mtu;

	assert(c != NULL);
	
	do{
		m = &c->msglist[c->msgused - 1];
		
		limit_to_mtu = (1 == c->msgused);	
		if(m->msg_iovlen == IOV_MAX)
		{
			add_msghdr(c);
			m = &c->msglist[c->msgused - 1];
		}

		if(ensure_iov_space(c) != 0)
			return -1;

		leftover = 0;
		
		m = &c->msglist[c->msgused - 1];
		m->msg_iov[m->msg_iovlen].iov_base = (void *)buf;
		m->msg_iov[m->msg_iovlen].iov_len = len;

		c->msgbytes += len;
		c->iovused++;
		m->msg_iovlen++;

		buf = ((char *)buf) + len;
		len = leftover;
	}while(leftover > 0);

	return 0;
}

static int read_network(conn *c)
{
	enum try_read_result gotdata = READ_NO_DATA_RECEIVED;
	int res;
	int num_allocs = 0;
	assert(c != NULL);

	if(c->rcurr != c->rbuf)
	{
		if(c->rbytes != 0)
			memmove(c->rbuf, c->rcurr, c->rbytes);
		c->rcurr = c->rbuf;
	}

	while(1)
	{
		if(c->rbytes >= c->rsize)
		{
			if(num_allocs == 4)
			{
				return gotdata;
			}
			++num_allocs;
			char *new_rbuf = realloc(c->rbuf, c->rsize * 2);
			if(!new_rbuf)
			{
				fprintf(stderr, "Couldn't realloc input buffer\n");
				c->rbytes = 0;
				c->write_and_go = conn_closing;
				return READ_MEMORY_ERROR;
			}
			c->rcurr = c->rbuf = new_rbuf;
			c->rsize *= 2;
		}

		int avail = c->rsize - c->rbytes;
		res = read(c->sfd, c->rbuf + c->rbytes, avail);
		if(res > 0)
		{
			printf("the avail is %d, the res is %d\n", avail, res);
			pthread_mutex_lock(&c->thread->stats.mutex);
			c->thread->stats.bytes_read += res;
			pthread_mutex_unlock(&c->thread->stats.mutex);
			gotdata = READ_DATA_RECEIVED;
			c->rbytes += res;
			if(res == avail)	
			{
				continue;
			}
			else
				break;
		}
		else if(res == 0)
		{
			return READ_ERROR;
		}
		else if(res == -1)
		{
			if(errno == EAGAIN || errno == EWOULDBLOCK)
				break;
			return READ_ERROR;
		}
	}

	return gotdata;
}

static void conn_set_state(conn *c, int state)
{
	assert(c != NULL);
	assert(state >= conn_listening && state < conn_max_state);

	if(state != c->state)
	{
		c->state = state;
	}
}

static int update_event(conn *c, const int new_flags)
{
	assert(c != NULL);

	struct event_base *base = c->event.ev_base;
	if(c->ev_flags == new_flags)
	{
		return 1;
	}

	if(event_del(&c->event) == -1)
	{
		printf("The event_del faild!\n");
		return 0;
	}

	event_set(&c->event, c->sfd, new_flags, event_handler, (void *)c);
	event_base_set(base, &c->event);
	c->ev_flags = new_flags;
	if(event_add(&c->event, 0) == -1)
	{
		printf("The event_del faild!\n");
		return 0;
	}

	return 1;
}

static void conn_shrink(conn *c)
{
	assert(c != NULL);

	if(c->rsize > READ_BUFFER_HIGHWAT && c->rbytes < DATA_BUFFER_SIZE)
	{
		char *newbuf;

		if(c->rcurr != c->rbuf)
			memmove(c->rbuf, c->rcurr, (size_t)c->rbytes);

		newbuf = (char *)realloc((void *)c->rbuf, DATA_BUFFER_SIZE);
	
		if(newbuf)
		{
			c->rbuf = newbuf;
			c->rsize = DATA_BUFFER_SIZE;
		}
		c->rcurr = c->rbuf;
	}

	if(c->isize > ITEM_LIST_HIGHWAT)
	{
		item **newbuf = (item **)realloc((void *)c->ilist, ITEM_LIST_INITIAL * sizeof(c->ilist[0]));
		if(newbuf)
		{
			c->ilist = newbuf;
			c->isize = ITEM_LIST_INITIAL;
		}
	}

	if(c->iovsize > IOV_LIST_HIGHWAT)
	{
		struct iovec *newbuf = (struct iovec *)realloc((void *)c->iov, IOV_LIST_INITIAL * sizeof(c->iov[0]));
		if(newbuf)
		{
			c->iov = newbuf;
			c->iovsize = IOV_LIST_INITIAL;
		}
	}
}

static void reset_cmd_handler(conn *c)
{
	c->cmd = -1;
	if(c->item != NULL)
	{
		c->item = NULL;//???
	}

	conn_shrink(c);
	printf("%s:%s->%d c->rbytes is %d\n", __FILE__, __func__, __LINE__, c->rbytes);
	if(c->rbytes > 0)
	{
		conn_set_state(c, conn_parse_cmd);
	}
	else
	{
		conn_set_state(c, conn_waiting);
	}
}	

static int transmit(conn *c)
{
	assert(c != NULL);

	if(c->msgcurr < c->msgused && c->msglist[c->msgcurr].msg_iovlen == 0)
	{
		c->msgcurr++;
	}

	printf("the c->msgcurr is %d, the c->msgused is %d\n", c->msgcurr, c->msgused);
	if(c->msgcurr < c->msgused)
	{
		ssize_t res;
		struct msghdr *m = &c->msglist[c->msgcurr];
		
		res = sendmsg(c->sfd, m, 0);
		if(res > 0)
		{
			pthread_mutex_lock(&c->thread->stats.mutex);
			c->thread->stats.bytes_written += res;
			pthread_mutex_unlock(&c->thread->stats.mutex);
		
			while(m->msg_iovlen > 0 && res >= m->msg_iov->iov_len)
			{
				res -= m->msg_iov->iov_len;
				m->msg_iovlen--;
				m->msg_iov++;
			}

			if(res > 0)
			{
				m->msg_iov->iov_base = (caddr_t)m->msg_iov->iov_base + res;
				m->msg_iov->iov_len -= res;
			}
			return TRANSMIT_INCOMPLETE;
		}

		if(res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
		{
			if(!update_event(c, EV_WRITE | EV_PERSIST))
			{
				printf("This come from %s:%s->%d Couldn't update event\n", __FILE__, __func__, __LINE__);
				conn_set_state(c, conn_closing);
				return TRANSMIT_HARD_ERROR;
			}
			return TRANSMIT_SOFT_ERROR;
		}

		conn_set_state(c, conn_closing);
		return TRANSMIT_HARD_ERROR;
	}
	else
	{
		return TRANSMIT_COMPLETE;
	}
}

static void conn_release_items(conn *c)
{
	assert(c != NULL);
	
	if(c->item)
	{
		item_remove(c->item);
        c->item = 0;
	}

	while (c->ileft > 0) {
        item *it = *(c->icurr);
        assert((it->it_flags & ITEM_SLABBED) == 0);
        item_remove(it);
        c->icurr++;
        c->ileft--;
    }

    c->icurr = c->ilist;
    c->suffixcurr = c->suffixlist;
}

static void drive_machine(conn *c)
{
	int stop = 0;
	int sfd;
	socklen_t addrlen;
	struct sockaddr_storage addr;
	int nreas = reqs_per_event;
	int res;
	const char *str;
	
	assert(c != NULL);
	while(!stop)
	{
		switch(c->state)
		{
			case conn_listening:
				addrlen = sizeof(addr);
				sfd = accept(c->sfd, (struct sockaddr *)&addr, &addrlen);
				if(sfd == -1)
				{
					perror("accept");
					if(errno == EAGAIN || errno == EWOULDBLOCK)
					{
						stop = 1;
					}
					else if(errno == EMFILE)
					{
						stop = 1;
					}
					break;
				}

				if(fcntl(sfd, F_SETFL, fcntl(sfd, F_GETFL) | O_NONBLOCK) < 0)
				{
					perror("setting O_NONBLOCK");
					close(sfd);
					break;
				}
				dispatch_conn_new(sfd, conn_new_cmd, EV_READ | EV_PERSIST, DATA_BUFFER_SIZE);
				printf("this come from %s:%s->%d, conn_listening\n", __FILE__, __func__, __LINE__);
				stop = 1;
				break;
			case conn_new_cmd:
				printf("this come from %s:%s->%d, the nreas is %d conn_new_cmd\n", __FILE__, __func__, __LINE__, nreas);
				--nreas;
				if(nreas >= 0)
				{
					reset_cmd_handler(c);
				}
				else
				{
					pthread_mutex_lock(&c->thread->stats.mutex);
					c->thread->stats.conn_yields++;
					pthread_mutex_unlock(&c->thread->stats.mutex);
					if(c->rbytes > 0)
					{
						if(!update_event(c, EV_WRITE | EV_PERSIST))
						{
							printf("This come from %s:%s->%d Couldn't update event\n", __FILE__, __func__, __LINE__);
							conn_set_state(c, conn_closing);
							break;
						}
						stop = 1;
					}
				}
				break;
			case conn_waiting:
				printf("this come from %s:%s->%d, conn_waiting\n", __FILE__, __func__, __LINE__);
				if(!update_event(c, EV_READ | EV_PERSIST))
				{
					conn_set_state(c, conn_closing);
					break;
				}
				
				conn_set_state(c, conn_read);
				stop = 1;
				break;
			case conn_read:
				res = read_network(c);	
				switch(res)
				{
					case READ_NO_DATA_RECEIVED:
						conn_set_state(c, conn_waiting);
						break;
					case READ_DATA_RECEIVED:
						conn_set_state(c, conn_parse_cmd);
						break;
					case READ_ERROR:
						printf("This come from %s->%d READ_ERROR\n", __FILE__, __LINE__);
						conn_set_state(c, conn_closing);
						break;
					case READ_MEMORY_ERROR:
						break;
				}
				break;
			case conn_parse_cmd:
				printf("this come from %s:%s->%d, conn_parse_cmd, the buff is %s\n", __FILE__, __func__, __LINE__, c->rcurr);
				add_msghdr(c);
				add_iov(c, c->rcurr, strlen(c->rcurr));
				c->rbytes = 0;
				if(!memcmp(c->rcurr, "Welcome to New World", 20))
					sleep(5);//add by 20151211

				memset(c->rbuf, 0, sizeof(c->rbuf));
				conn_set_state(c, conn_mwrite);
				break;
			case conn_nread:
				if(c->rlbytes == 0)
				{
					break;
				}
			
				if(c->rlbytes < 0)
				{
					fprintf(stderr, "Invalid rlbytes to read: len %d\n", c->rlbytes);
					conn_set_state(c, conn_closing);
					break;
				}

				if(c->rlbytes > 0)
				{
					int tocopy = c->rbytes > c->rlbytes ? c->rlbytes : c->rbytes;
					if(c->ritem != c->rcurr)
					{
						memmove(c->ritem, c->rcurr, tocopy);
					}
					c->ritem += tocopy;
					c->rlbytes -= tocopy;
					c->rcurr += tocopy;
					c->rbytes -= tocopy;
					if(c->rlbytes == 0)
						break;
				}

				res = read(c->sfd, c->ritem, c->rlbytes);
				if(res > 0)
				{
					pthread_mutex_lock(&c->thread->stats.mutex);
					c->thread->stats.bytes_read += res;
					pthread_mutex_unlock(&c->thread->stats.mutex);
					if(c->rcurr == c->ritem)
						c->rcurr += res;

					c->ritem += res;
					c->rlbytes -= res;
					break;
				}

				if(res == 0)
				{
					conn_set_state(c, conn_closing);
					break;
				}

				if(res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
				{
					if(!update_event(c, EV_READ | EV_PERSIST))
					{
						printf("This come from %s:%s->%d Couldn't update event\n", __FILE__, __func__, __LINE__);
						conn_set_state(c, conn_closing);
					}
					stop = 1;
					break;
				}
				conn_set_state(c, conn_closing);
				break;
			case conn_write:
				/*
				* We want to write out a simple response. If we haven't already,
				* assemble it into a msgbuf list (this will be a single-entry
				* list for TCP or a two-entry list for UDP).
				*/
				if(c->iovused == 0)
				{
					if(add_iov(c, c->wcurr, c->wbytes) != 0)
					{
						fprintf(stderr, "Couldn't build response\n");
						conn_set_state(c, conn_closing);
						break;
					}
				}
			case conn_mwrite:
				printf("this come from %s:%s->%d c->rbytes is %d conn_mwrite\n", __FILE__, __func__, __LINE__, c->rbytes);
				switch(transmit(c))
				{
					case TRANSMIT_COMPLETE:
						if (c->state == conn_mwrite) 
						{
							conn_release_items(c);
							conn_set_state(c, conn_new_cmd);
						} 
						else if (c->state == conn_write) 
						{
							if (c->write_and_free) 
							{
								free(c->write_and_free);
								c->write_and_free = 0;
							}
							conn_set_state(c, c->write_and_go);
						} 
						else 
						{
							fprintf(stderr, "Unexpected state %d\n", c->state);
							conn_set_state(c, conn_closing);
						}
						break;

					case TRANSMIT_INCOMPLETE:
					case TRANSMIT_HARD_ERROR:
						break;                   /* Continue in state machine. */

					case TRANSMIT_SOFT_ERROR:
						stop = 1;
						break;
				}
				break;
			case conn_closing:
				conn_close(c);
				stop = 1;
				break;
			default:
				break;
		}
	}
	printf("this come from %s:%s->%d, drive_machine existing\n", __FILE__, __func__, __LINE__);

	return;
}

void event_handler(const int fd, const short which, void *arg)
{
	conn *c;
	c = (conn *)arg;
	assert(c != NULL);

	c->which = which;

	if(fd != c->sfd)
	{	
		printf("%s faild!\n", __func__);
		conn_close(c);
		return;
	}

	printf("this come from %s, the fd is %d, the c->sfd is %d, the c->state is %d\n", __func__, fd, c->sfd, c->state);
	drive_machine(c);
	return;
}

void conn_init(void)
{
	if((conns = calloc(MAX_FD, sizeof(conn *))) == NULL)
	{
		perror("calloc");
		exit(1);
	}	
}

void conn_free(conn *c)
{
	if(c)
	{
		assert(c != NULL);
        assert(c->sfd >= 0);

		conns[c->sfd] = NULL;
        if (c->msglist)
            free(c->msglist);
        if (c->rbuf)
            free(c->rbuf);
        if (c->wbuf)
            free(c->wbuf);
        if (c->ilist)
            free(c->ilist);
        if (c->suffixlist)
            free(c->suffixlist);
        if (c->iov)
            free(c->iov);
        free(c);
	}
}

conn *conn_new(const int sfd, int init_state, const int event_flags, const int read_buffer_size, struct event_base *base)
{
	conn *c;
	
	assert(sfd >= 0);
	c = conns[sfd];

	if (NULL == c) 
	{
        if (!(c = (conn *)calloc(1, sizeof(conn)))) 
		{
            fprintf(stderr, "Failed to allocate connection object\n");
            return NULL;
        }

        c->rbuf = c->wbuf = 0;
        c->ilist = 0;
        c->suffixlist = 0;
        c->iov = 0;
        c->msglist = 0;

        c->rsize = read_buffer_size;
        c->wsize = DATA_BUFFER_SIZE;
        c->isize = ITEM_LIST_INITIAL;
        c->suffixsize = SUFFIX_LIST_INITIAL;
        c->iovsize = IOV_LIST_INITIAL;
        c->msgsize = MSG_LIST_INITIAL;
        c->hdrsize = 0;

        c->rbuf = (char *)malloc((size_t)c->rsize);
        c->wbuf = (char *)malloc((size_t)c->wsize);
        c->ilist = (item **)malloc(sizeof(item *) * c->isize);
        c->suffixlist = (char **)malloc(sizeof(char *) * c->suffixsize);
        c->iov = (struct iovec *)malloc(sizeof(struct iovec) * c->iovsize);
        c->msglist = (struct msghdr *)malloc(sizeof(struct msghdr) * c->msgsize);

        if (c->rbuf == 0 || c->wbuf == 0 || c->ilist == 0 || c->iov == 0 ||
                c->msglist == 0 || c->suffixlist == 0) {
            conn_free(c);
            fprintf(stderr, "Failed to allocate buffers for connection\n");
            return NULL;
        }

#if 0
		pthread_mutex_lock(&stats_lock);
        stats.conn_structs++;
		pthread_mutex_unlock(&stats_lock);
#endif

        c->sfd = sfd;
        conns[sfd] = c;
    }

    c->state = init_state;
    c->rlbytes = 0;
    c->cmd = -1;
    c->rbytes = c->wbytes = 0;
    c->wcurr = c->wbuf;
    c->rcurr = c->rbuf;
    c->ritem = 0;
    c->icurr = c->ilist;
    c->suffixcurr = c->suffixlist;
    c->ileft = 0;
    c->suffixleft = 0;
    c->iovused = 0;
    c->msgcurr = 0;
    c->msgused = 0;
    c->authenticated = 0;

    c->write_and_go = init_state;
    c->write_and_free = 0;
    c->item = 0;

    c->noreply = 0;
	
    event_set(&c->event, sfd, event_flags, event_handler, (void *)c);
    event_base_set(base, &c->event);
    c->ev_flags = event_flags;

    if (event_add(&c->event, 0) == -1) {
        perror("event_add");
        return NULL;
    }

    return c;
}

int server_socket(int port, struct event_base *main_base)
{
	struct sockaddr_in sin;
	struct linger ling = {0, 0};
	int flags = 1;
	int fd = 0;
	int error;
	
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = 0;
	sin.sin_port = htons(port);

	fd = socket(AF_INET, SOCK_STREAM, 0);
	assert(fd > 0);

	setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
	error = setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
	if (error != 0)
		perror("setsockopt");

	error = setsockopt(fd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
	if (error != 0)
		perror("setsockopt");

	error = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));
	if (error != 0)
		perror("setsockopt");	

	if(bind(fd, (struct sockaddr *)&sin, sizeof(sin)) < 0)
	{
		perror("bind");
        return -1;
	}

	if(listen(fd, 16)<0)
    {
        perror("listen");
        return -2;
    }

	if(!conn_new(fd, conn_listening, EV_READ | EV_PERSIST, 1, main_base))
	{
		fprintf(stderr, "failed to create listening connection\n");
		return -3;
	}

	return 0;
}
