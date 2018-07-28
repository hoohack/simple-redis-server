/* Redis Object implementation.
 * Redis对象实现
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "zmalloc.h"
#include <math.h>
#include <ctype.h>
#include <string.h>

#ifdef __CYGWIN__
#define strtold(a,b) ((long double)strtod((a),(b)))
#endif

/* ===================== Creation and parsing of objects ==================== */

robj *createObject(int type, void *ptr) {
	robj *o = zmalloc(sizeof(*o));
	o->type = type;
	o->encoding = OBJ_ENCODING_RAW;
	o->ptr = ptr;
	o->refcount = 1;

	return o;
}

/* Create a string object with encoding OBJ_ENCODING_RAW, that is a plain
 * string object where o->ptr points to a proper sds string. */
/*
 * 创建一个字符串对象
 * 编码类型是OBJ_ENCODING_RAW
 * ptr指向sds字符串
 */
robj *createRawStringObject(const char *ptr, size_t len) {
	return createObject(OBJ_STRING, sdsnewlen(ptr,len));
}

/* Create a string object with encoding OBJ_ENCODING_EMBSTR, that is
 * an object where the sds string is actually an unmodifiable string
 * allocated in the same chunk as the object itself. */
/*
 * 创建一个使用EMBSTR编码的字符串对象
 * EMBSTR字符串是不可被修改的，在分配空间时，会分配一块连续的空间保存redis对象和字符串对象
 */
robj *createEmbeddedStringObject(const char *ptr, size_t len) {
	robj *o = zmalloc(sizeof(robj)+sizeof(struct sdshdr8)+len+1);
	struct sdshdr8 *sh = (void*)(o+1);

	o->type = OBJ_STRING; // 设置对象类型
	o->encoding = OBJ_ENCODING_EMBSTR; // 设置对象编码
	o->ptr = sh+1; // 指向数据
	o->refcount = 1; // 初始化，引用计数设置为1

	sh->len = len;
	sh->alloc = len;
	sh->flags = SDS_TYPE_8; // 设置字符串类型标志
	if (ptr) {
		// 字符串不为空，拷贝数据到buf中
		memcpy(sh->buf,ptr,len);
		sh->buf[len] = '\0';
	} else {
		// 字符串为空
		memset(sh->buf,0,len+1);
	}
	return o;
}

/* Create a string object with EMBSTR encoding if it is smaller than
 * OBJ_ENCODING_EMBSTR_SIZE_LIMIT, otherwise the RAW encoding is
 * used.
 *
 * The current limit of 39 is chosen so that the biggest string object
 * we allocate as EMBSTR will still fit into the 64 byte arena of jemalloc. */
/*
 * 创建一个字符串对象
 * 如果字符串长度小于OBJ_ENCODING_EMBSTR_SIZE_LIMIT，使用EMBSTR编码
 * 否则使用RAW动态字符串编码
 */
#define OBJ_ENCODING_EMBSTR_SIZE_LIMIT 44
robj *createStringObject(const char *ptr, size_t len) {
	if (len <= OBJ_ENCODING_EMBSTR_SIZE_LIMIT)
		return createEmbeddedStringObject(ptr,len);
	else
		return createRawStringObject(ptr,len);
}


/*
 * 释放对象空间系列函数
 * ---begin---
 */
void freeStringObject(robj *o) {
	if (o->encoding == OBJ_ENCODING_RAW) {
		sdsfree(o->ptr);
	}
}

/*
 * 释放对象空间系列函数
 * ---end---
 */

/*
 * 增加对象的引用值
 * OBJ_SHARED_REFCOUNT 是共享对象的引用值，如果是此类共享对象，就不修改它
 */
void incrRefCount(robj *o) {
	if (o->refcount != OBJ_SHARED_REFCOUNT) o->refcount++;
}

/*
 * 对象的引用值减1
 * 如果对象当前引用值是1，根据对象的类型调用相应的释放对象函数
 * OBJ_SHARED_REFCOUNT 是共享对象的引用值，如果是此类共享对象，就不修改它
 */
void decrRefCount(robj *o) {
	if (o->refcount == 1) {
		switch(o->type) {
			case OBJ_STRING: freeStringObject(o); break;
			default: break;
		}
		zfree(o);
	} else {
		if (o->refcount != OBJ_SHARED_REFCOUNT) o->refcount--;
	}
}

/* This variant of decrRefCount() gets its argument as void, and is useful
 * as free method in data structures that expect a 'void free_object(void*)'
 * prototype for the free method. */
/*
 * 这是decrRefCount函数的变形，传入的参数是一个空指针，适合对传入各种类型的对象进行减少引用的操作
 */
void decrRefCountVoid(void *o) {
	decrRefCount(o);
}
