#include "server.h"
#include "dict.h"
#include "sds.h"

#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/time.h>
#include <pthread.h>
#include <time.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#include <sys/resource.h>
#include <sys/utsname.h>
#include <locale.h>
#include <sys/socket.h>

void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask);

/*================================= Globals ================================= */

/* Global vars */
struct redisServer server; /* Server global state */

void setCommand(client *c);
void getCommand(client *c);
void commandCommand(client *c);

void commandCommand(client *c) {
	dictIterator *di;
	dictEntry *de;

	if (c->argc == 1) {
		di = dictGetIterator(server.commands);
		dictReleaseIterator(di);
	}
}

struct redisCommand redisCommandTable[] = {
	{"get",getCommand,2,"rF",0,NULL,1,1,1,0,0},
	{"set",setCommand,-3,"wm",0,NULL,1,1,1,0,0},
	{"command",commandCommand,0,"lt",0,NULL,0,0,0,0,0}
};

/* A case insensitive version used for the command lookup table and other
 * places where case insensitive non binary-safe comparison is needed. */
int dictSdsKeyCaseCompare(void *privdata, const void *key1,
		const void *key2)
{
	DICT_NOTUSED(privdata);

	return strcasecmp(key1, key2) == 0;
}

/*
 * 根据给定命令名字（SDS），查找命令
 */
struct redisCommand *lookupCommand(sds name) {
	return dictFetchValue(server.commands, name);
}

void call(client *c, int flags) {
	printf("cmd name :%s\n", c->cmd->name);
	c->cmd->proc(c); // 执行实现函数
}

int processCommand(client *c) {
	/*
	 * 访问redis的命令表，查找命令
	 * 然后检查参数是否错误
	 */
	c->cmd = c->lastcmd = lookupCommand(c->argv[0]->ptr);
	call(c,CMD_CALL_FULL);
	return C_OK;
}

void dictSdsDestructor(void *privdata, void *val)
{
	DICT_NOTUSED(privdata);

	sdsfree(val);
}

uint64_t dictSdsCaseHash(const void *key) {
	return dictGenCaseHashFunction((unsigned char*)key, sdslen((char*)key));
}

/* Command table. sds string -> command struct pointer. */
dictType commandTableDictType = {
	dictSdsCaseHash,           /* hash function */
	NULL,                      /* key dup */
	NULL,                      /* val dup */
	dictSdsKeyCaseCompare,     /* key compare */
	dictSdsDestructor,         /* key destructor */
	NULL                       /* val destructor */
};

/* Populates the Redis Command Table starting from the hard coded list
 * we have on top of redis.c file. */
void populateCommandTable(void) {
	int j;
	int numcommands = sizeof(redisCommandTable)/sizeof(struct redisCommand);

	for (j = 0; j < numcommands; j++) {
		struct redisCommand *c = redisCommandTable+j;
		char *f = c->sflags;
		int retval1, retval2;

		while(*f != '\0') {
			switch(*f) {
				case 'w': c->flags |= CMD_WRITE; break;
				case 'r': c->flags |= CMD_READONLY; break;
				case 'm': c->flags |= CMD_DENYOOM; break;
				case 'a': c->flags |= CMD_ADMIN; break;
				case 'p': c->flags |= CMD_PUBSUB; break;
				case 's': c->flags |= CMD_NOSCRIPT; break;
				case 'R': c->flags |= CMD_RANDOM; break;
				case 'S': c->flags |= CMD_SORT_FOR_SCRIPT; break;
				case 'l': c->flags |= CMD_LOADING; break;
				case 't': c->flags |= CMD_STALE; break;
				case 'M': c->flags |= CMD_SKIP_MONITOR; break;
				case 'k': c->flags |= CMD_ASKING; break;
				case 'F': c->flags |= CMD_FAST; break;
				default: break;
			}
			f++;
		}

		retval1 = dictAdd(server.commands, sdsnew(c->name), c);
		/* Populate an additional dictionary that will be unaffected
		 * by rename-command statements in redis.conf. */
		retval2 = dictAdd(server.orig_commands, sdsnew(c->name), c);
	}
}

/*
 * 初始化redisServer变量
 */
void initServerConfig(void) {
	int j;

	// 初始化其他属性
	server.hz = CONFIG_DEFAULT_HZ;
	server.arch_bits = (sizeof(long) == 8) ? 64 : 32;
	server.port = CONFIG_DEFAULT_SERVER_PORT;
	server.tcp_backlog = CONFIG_DEFAULT_TCP_BACKLOG;
	server.bindaddr_count = 0;

	server.ipfd_count = 0;
	server.dbnum = CONFIG_DEFAULT_DBNUM;
	server.tcpkeepalive = CONFIG_DEFAULT_TCP_KEEPALIVE;
	server.maxclients = CONFIG_DEFAULT_MAX_CLIENTS;

	/* 创建命令表
	 * Command table -- we initiialize it here as it is part of the
	 * initial configuration, since command names may be changed via
	 * redis.conf using the rename-command directive. */
	server.commands = dictCreate(&commandTableDictType,NULL);
	server.orig_commands = dictCreate(&commandTableDictType,NULL);

	populateCommandTable(); // 加载命令表
}

void closeListeningSockets(int unlink_unix_socket) {
	int j;
	for (j = 0; j < server.ipfd_count; j++) close(server.ipfd[j]);
}

int prepareForShutdown(int flags) {
	// 关闭监听套接字,这样在重启的时候会快一点
	closeListeningSockets(1);
	return C_OK;
}

void clientsCron(void) {
	// todo
}

int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
	int j;
	// 服务器进程收到 SIGTERM 消息,关闭服务器
	if (server.shutdown_asap) {
		// 尝试关闭服务器
		if (prepareForShutdown(0) == C_OK) exit(0);
		// 运行到这里说明关闭失败
		server.shutdown_asap = 0;
	}
	// 检查客户端,关闭超时的客户端,并释放客户端多余的缓冲区
	clientsCron();
	return 1000000 / server.hz; // 这个返回的值决定了下次什么时候再调用这个函数
}

static void sigtermHandler(int sig) {
	// todo
}

/*
 * 设置信号处理函数
 */
void setupSignalHandlers(void) {
	struct sigaction act;
	sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	act.sa_handler = sigtermHandler;
	sigaction(SIGTERM, &act, NULL);

}

int listenToPort(int port, int *fds, int *count) {
	int j;

	/* Force binding of 0.0.0.0 if no bind address is specified, always
	 * entering the loop if j == 0. */
	if (server.bindaddr_count == 0) server.bindaddr[0] = NULL;
	for (j = 0; j < server.bindaddr_count || j == 0; j++) {
		if (server.bindaddr[j] == NULL) {
			int unsupported = 0;
			/* Bind * for both IPv6 and IPv4, we enter here only if
			 * server.bindaddr_count == 0. */
			fds[*count] = anetTcp6Server(server.neterr,port,NULL,
					server.tcp_backlog);
			if (fds[*count] != ANET_ERR) {
				anetNonBlock(NULL,fds[*count]);
				(*count)++;
			} else if (errno == EAFNOSUPPORT) {
				unsupported++;
			}

			if (*count == 1 || unsupported) {
				/* Bind the IPv4 address as well. */
				fds[*count] = anetTcpServer(server.neterr,port,NULL,
						server.tcp_backlog);
				if (fds[*count] != ANET_ERR) {
					anetNonBlock(NULL,fds[*count]);
					(*count)++;
				} else if (errno == EAFNOSUPPORT) {
					unsupported++;
				}
			}
			/* Exit the loop if we were able to bind * on IPv4 and IPv6,
			 * otherwise fds[*count] will be ANET_ERR and we'll print an
			 * error and return to the caller with an error. */
			if (*count + unsupported == 2) break;
		} else if (strchr(server.bindaddr[j],':')) {
			/* Bind IPv6 address. */
			fds[*count] = anetTcp6Server(server.neterr,port,server.bindaddr[j],
					server.tcp_backlog);
		} else {
			/* Bind IPv4 address. */
			fds[*count] = anetTcpServer(server.neterr,port,server.bindaddr[j],
					server.tcp_backlog);
		}
		if (fds[*count] == ANET_ERR) {
			return C_ERR;
		}
		anetNonBlock(NULL,fds[*count]);
		(*count)++;
	}
	return C_OK;
}

/*
 * 初始化服务器
 */
void initServer(void) {
	int j;

	signal(SIGHUP, SIG_IGN);
	signal(SIGPIPE, SIG_IGN);
	// 设置进程信号处理器
	setupSignalHandlers();

	server.clients = listCreate(); // 客户端链表
	server.clients_to_close = listCreate();
	/* 初始化事件循环 */
	server.el = aeCreateEventLoop(server.maxclients+CONFIG_FDSET_INCR);
	if (server.el == NULL) {
		exit(1);
	}

	/* 打开TCP监听套接字 */
	if (server.port != 0 &&
			listenToPort(server.port,server.ipfd,&server.ipfd_count) == C_ERR)
		exit(1);

	/*
	 * 注册时间事件定时器
	 * 这是redis处理后台操作的方法，比如客户端超时，删除超时的key等等
	 */
	if (aeCreateTimeEvent(server.el, 1, serverCron, NULL, NULL) == AE_ERR) {
		exit(1);
	}

	/*
	 * 注册文件事件句柄，接收新的请求，新请求到来时调用acceptHandler函数
	 */
	for (j = 0; j < server.ipfd_count; j++) {
		if (aeCreateFileEvent(server.el, server.ipfd[j], AE_READABLE,
					acceptTcpHandler,NULL) == AE_ERR)
		{
			printf("error\n");
			exit(1);
		}
	}

}

/*
 * main，程序入口，server启动函数
 */
int main(int argc, char **argv) {
	struct timeval tv;
	int j;

	initServerConfig(); // 初始化服务器状态

	// 初始化服务器
	initServer();
	printf("*************init server done ************\n");
	// 启动事件循环器，开始监听事件
	aeMain(server.el);
	return 0;
}

/* The end */
