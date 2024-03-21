使用Go语言实现的简易Redis服务器。
已实现的功能包括：
- 使用redis client建立TCP连接
- 密码认证
- 实现RESP协议
- String 数据类型命令
- 部分Generic命令

已实现的命令包括：
- string类型所有命令
- set类型所有命令
- list部分命令
- generic部分命令，包括：keys,del,exists,expire,expireat,pexpire,pexireat,expiretime,pexpiretime,ttl,pttl
- system部分命令，包括info，ping,auth

没打算实现的功能:
- 事务
- 集群
- 哨兵
