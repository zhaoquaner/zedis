使用Go语言实现的简易Redis服务器。
已实现的功能包括：
- 使用redis client建立TCP连接
- 密码认证
- 实现RESP协议
- String、List、Set、Generic、System部分命令

已实现的命令包括：
- string类型所有命令
- set类型所有命令
- list部分命令
- generic部分命令
- system部分命令

没打算实现的功能:
- 事务
- 集群
- 哨兵
