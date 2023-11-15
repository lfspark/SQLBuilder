# SQLBuilder
根据pg数据库的AST抽象语法树和配置生成可执行sql

由于AST抽象语法树，算法能力要求太高，跪求算法高人帮助，指点
已知规则如下：
1、字符串中[字段]表示可有可没有字段
2、字符串中{字段}表示是一个短语
3、字符串中 | 表示是前面短语或后面短语
4、字符串中[...]表示之前字段有n多个，至少有1个
5、字符串中[,...]表示之前字段有n多个，并且n个之间用逗号分割。至少有1个
6、字符串中可以随机组合[]{}|[...][,...]
例如：
字符串为 create [ [ GLOBAL | LOCAL ] { TEMPORARY | TEMP } | UNLOGGED ] TABLE
输出列表为 
create GLOBAL TEMPORARY TABLE
create GLOBAL TEMP TABLE
create UNLOGGED TABLE
create  TABLE
