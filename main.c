#include "common.h"

static struct event_base *main_base;

int main(void)
{
	main_base = event_init();
	assert(main_base != NULL);

	conn_init();

	memcached_thread_init(THREAD_NUM, main_base);
	server_socket(10000, main_base);

	if(event_base_loop(main_base, 0) != 0)
	{
		printf("event_base_loop faild!\n");
		exit(0);
	}

	return 0;
}
