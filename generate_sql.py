import re
from enum import Enum
from mako.template import Template
from mako.lookup import TemplateLookup
from functools import reduce


class temp_table_commit_behavior(Enum):
    val1 = ' PRESERVE ROWS '
    val2 = ' DELETE ROWS '
    val3 = ' DROP '
    val4 = ''


def cluster(table_name='', index_name='', verbose=False):
    sql_clause_cluster = '''
<%doc>Command:     CLUSTER
Description: cluster a table according to an index
Syntax:
</%doc>
CLUSTER 
## 是否有VERBOSE
% if verbose: 
VERBOSE 
% endif 
## 是否有表名
% if table_name: 
${table_name} 
% endif 
## 是否有索引
% if index_name: 
USING ${index_name}
% endif '''

    t = Template(sql_clause_cluster)
    return t.render(verbose=verbose, table_name=table_name, index_name=index_name).replace('\n', '')


def create_table(table_name, column, temp='', unlogged='', partition_parent_table={}, table_constraint='', \
                 like_source_table='', inherits_tablename='', ifnotexits=0, like_option='', \
                 partition_table={}, table_store_para='', without_oid_display=0,
                 temp_table_on_commit_behavior: temp_table_commit_behavior = '', \
                 tablespace=''):
    table_type = 0
    # 0 表示普通表
    # 1 表示range分区表
    # 2 表示list分区表
    # 3 表示hash分区表
    # 4 表示继承表
    # 5 表示临时分区表
    # 6 表示hash分区表
    sql_clause_create_table = '''
<%!
from random import random
def random_bool():
  return bool(round(random()))
%>
<%doc>Command:     CREATE TABLE
Description: 建立新的数据表
Syntax:
</%doc>
CREATE 
%if temp:
    %if str(temp).upper().strip()=='GLOBAL':
        GLOBAL
    %elif str(temp).upper().strip()=='LOCAL':
        LOCAL
    %endif

    %if random_bool():
        TEMPORARY 
    %else:
        TEMP 
    %endif

%endif

%if unlogged:
    UNLOGGED 
%endif

TABLE 

%if ifnotexits:
    IF NOT EXISTS 
%endif 

${table_name} 

%if not partition_parent_table:
( 
    <% col_count=0%>
    %if column :
        %for i in column:
            <% col_count+=1%>
           ${i} ${column[i]['datatype']}
            %if 'collate' in column[i] and column[i]['collate']:
                COLLATE  ${column[i]['collate']} 
            %endif
            
            %if 'constraint' in column[i] and column[i]['constraint']:
                ${column[i]['constraint']} 
            %endif
            
            %if  col_count<len(column):
                , 
            %endif
            
        %endfor
        
    %endif
       
    %if table_constraint:
        , ${table_constraint}
    %endif
    
    %if like_source_table:
        , LIKE ${like_source_table} 
        %if like_option:
            ${like_option} 
        %endif
    %endif
     )
     
        
    %if inherits_tablename:
        INHERITS ( ${','.join(inherits_tablename)} ) 
    %endif

%endif
## 子分区表创建####################################################################
%if partition_parent_table:
    PARTITION OF ${partition_parent_table['name']} 

    %if str(partition_parent_table['parttype']).strip().upper()=='RANGE':
    
        %if len(partition_parent_table['value'])==2:
            FOR VALUES from (${partition_parent_table['value'][0]}) to (${partition_parent_table['value'][1]})
        %else :
            FOR VALUES default;
        %endif
    
    %elif str(partition_parent_table['parttype']).strip().upper()=='LIST':
    
        %if len(partition_parent_table['value'])>0:
            FOR VALUES in (${','.join(partition_parent_table['value'])}) 
        %else :
            FOR VALUES default;
        %endif
    
    %elif str(partition_parent_table['parttype']).strip().upper()=='HASH':
        FOR VALUES WITH ( MODULUS ${partition_parent_table['value'][0]} , REMAINDER ${partition_parent_table['value'][1]} )
    
    %endif
%endif

##########创建分区表 #####################
%if partition_table:
    %if str(partition_table['parttype']).upper().strip()=='RANGE':
        PARTITION BY RANGE
    %elif str(partition_table['parttype']).upper().strip()=='LIST':
        PARTITION BY LIST
    %elif str(partition_table['parttype']).upper().strip()=='HASH':
        PARTITION BY HASH
    %endif
    
        ( ${partition_table['key']} 
    
    %if 'collate' in partition_table and partition_table['collate']:
        COLLATE ${partition_table['collate']} 
    %endif
    
    %if 'op' in partition_table and partition_table['op']:
         ${partition_table['op']} 
    %endif
     )
    
    %if 'index' in partition_table and partition_table['index']:
         USING ${partition_table['index']} 
    %endif
%endif

%if table_store_para:
    WITH ( ${table_store_para} ) 
%endif

%if without_oid_display:
    WITHOUT OIDS  
%endif

%if temp and temp_table_on_commit_behavior:
    ON COMMIT ${temp_table_on_commit_behavior}
%endif

%if tablespace :
    TABLESPACE ${tablespace} 
%endif

'''
    mylookup = TemplateLookup(directories=[r''])
    t = Template(sql_clause_create_table, lookup=mylookup)
    sql_statment = t.render(table_name=table_name, column=column, temp=temp, unlogged=unlogged,
                            table_constraint=table_constraint, \
                            like_source_table=like_source_table, inherits_tablename=inherits_tablename,
                            ifnotexits=ifnotexits, \
                            like_option=like_option, partition_parent_table=partition_parent_table,
                            partition_table=partition_table, \
                            table_store_para=table_store_para, without_oid_display=without_oid_display,
                            tablespace=tablespace, \
                            temp_table_on_commit_behavior=temp_table_on_commit_behavior
                            ).replace('\n', '')
    sql_statment = re.sub('\s\s+', ' ', sql_statment) + ';'
    return sql_statment


# print(create_table(table_name='a',column={'id': {'datatype':'int'},'name':{'datatype':'text','collate':'C','constraint':'not null primary key'}},partition_table={'parttype':'hash','key':'id','index':'btree'}))
# print(create_table(partition_parent_table={'name':'x','parttype':'hash','value':(4,1)},ifnotexits=1,table_name='b',column={'id': {'datatype':'int'},'name':{'datatype':'text','constraint':'not null primary key'}},like_source_table='a',inherits_tablename=('x','y')))


def create_view(view_name, query, replace=''):
    sql_clause_create_view = '''
    <%!
from random import random
def random_bool():
  return bool(round(random()))
%>
<%doc>Command:     CREATE VIEW
Description: define a new view
Syntax:
</%doc>

CREATE 
%if replace:
  or  REPLACE 
%endif

%if temp:
    %if random_bool:
        TEMP 
    %else:
        TEMPORARY 
    %endif
%endif 

%if recursize:
    RECURSIVE 
%endif 
VIEW ${view_name} 

## [ ( column_name [, ...] ) ] 未实现
##  [ WITH ( view_option_name [= view_option_value] [, ... ] ) ] 未实现
    AS ${query}
##   [ WITH [ CASCADED | LOCAL ] CHECK OPTION [ CONSTRAINT [ cons_name ] ] ] 未实现


'''
    mylookup = TemplateLookup(directories=[r''])
    t = Template(sql_clause_create_view, lookup=mylookup)
    sql_statment = t.render(query=query, view_name=view_name, replace=replace).replace('\n', '')
    sql_statment = re.sub('\s\s+', ' ', sql_statment) + ';'
    return sql_statment


# print(create_view(view_name='tt',query='select * from x',replace=1))

def drop_table(table_name, ifexists=1, cascade=1):
    sql_clause_create_view = '''<%doc>Command:     DROP TABLE
Description: remove a table
Syntax:
</%doc>
DROP TABLE 
%if ifexists:
 IF EXISTS 
%endif
 ${','.join(table_name)}
%if cascade:
    CASCADE
%endif
'''
    mylookup = TemplateLookup(directories=[r''])
    t = Template(sql_clause_create_view, lookup=mylookup)
    sql_statment = t.render(table_name=table_name, ifexists=ifexists, cascade=cascade).replace('\n', '')
    sql_statment = re.sub('\s\s+', ' ', sql_statment) + ';'
    return sql_statment


def drop_view(view_name, cascade=''):
    sql_clause_create_view = '''<%doc>Command:     DROP TABLE
Description: remove a table
Syntax:
</%doc>
DROP VIEW 
%if ifexists:
 IF EXISTS 
%endif
 ${','.join(view_name)}
%if cascade:
    CASCADE
%endif
'''
    mylookup = TemplateLookup(directories=[r''])
    t = Template(sql_clause_create_view, lookup=mylookup)
    sql_statment = t.render(view_name=view_name, cascade=cascade).replace('\n', '')
    sql_statment = re.sub('\s\s+', ' ', sql_statment) + ';'
    return sql_statment


def insert(table_name, table_alias='', column='', values: tuple = (), query=''):
    assert not (values and query), '语法错误'
    values = list(map(lambda x: str(x), values))
    sql_clause_create_view = '''<%doc>Command:     INSERT
Description: create new rows in a table
Syntax:
</%doc>
### [ WITH [ RECURSIVE ] with_query [, ...] ]未实现
INSERT INTO ${table_name} 
%if table_alias:
    AS ${table_alias} 
%endif
%if column:
    (${','.join(column)})
%endif
#####    [ OVERRIDING { SYSTEM | USER } VALUE ]未实现
%if not values and not query:
    DEFAULT VALUES 
%endif

%if values and not query:
    VALUES (${','.join(values)}) 
%endif

%if not values and  query:
    ${query} 
%endif
#################以下语法都未实现 ###################
######    [ ON CONFLICT [ conflict_target ] conflict_action ]未实现
####    [ RETURNING * | output_expression [ [ AS ] output_name ] [, ...] ]
####
######INSERT ALL { insert_into_clause [ values_clause ] } [, ...] query未实现
######INSERT { ALL | FIRST }未实现
######   { WHEN condition THEN { insert_into_clause [ values_clause ] } [, ...] } [, ...]未实现
######   [ ELSE {insert_into_clause [ values_clause ] } [, ...] ]未实现
####
######INSERT table_name SET assignment_list ON DUPLICATE KEY UPDATE assignment_list
####
######where assignment_list can be one of:
####
######    column_name = { constant_value | constant_expression } [, ...]
####
######where conflict_target can be one of:
####
######    ( { index_column_name | ( index_expression ) } [ COLLATE collation ] [ opclass ] [, ...] ) [ WHERE index_predicate ]
######    ON CONSTRAINT constraint_name
####
####and conflict_action is one of:
####
####    DO NOTHING
####    DO UPDATE SET { column_name = { expression | DEFAULT } |
####                    ( column_name [, ...] ) = [ ROW ] ( { expression | DEFAULT } [, ...] ) |
####                    ( column_name [, ...] ) = ( sub-SELECT )
####                  } [, ...]
####              [ WHERE condition ]
####
####and insert_into_clause is:
####    INTO table_name [ ( column_name [, ...] ) ]
####and values_clause is:
####    VALUES ( expression [, ...] )
'''
    mylookup = TemplateLookup(directories=[r''])
    t = Template(sql_clause_create_view, lookup=mylookup)
    sql_statment = t.render(table_name=table_name, table_alias=table_alias, column=column, values=values,
                            query=query).replace('\n', '')
    sql_statment = re.sub('\s\s+', ' ', sql_statment) + ';' + '\n'
    return sql_statment


def delete(table_name, hashfrom=1, only_inherits_parent=0, where_con=''):
    sql_clause_create_view = '''<%doc>Command:     DELETE
Description: delete rows of a table
Syntax:
</%doc>
####[ WITH [ RECURSIVE ] with_query [, ...] ]未实现
DELETE 
%if hashfrom:
 FROM 
%endif

%if only_inherits_parent:
 ONLY 
%endif
 ${table_name}  
##############    [ [ AS ] alias ]未实现
################    [ USING from_item [, ...] ]未实现
%if where_con:
    WHERE %{where_con} 
%endif

############### WHERE CURRENT OF cursor_name ]未实现
###############    [ ORDER BY column_name [ LIMIT limit_count ] ]未实现
###############     [ RETURNING * | output_expression [ [ AS ] output_name ] [, ...] ]未实现
'''
    mylookup = TemplateLookup(directories=[r''])
    t = Template(sql_clause_create_view, lookup=mylookup)
    sql_statment = t.render(table_name=table_name, hashfrom=hashfrom, only_inherits_parent=only_inherits_parent,
                            where_con=where_con).replace('\n', '')
    sql_statment = re.sub('\s\s+', ' ', sql_statment) + ';'
    return sql_statment


def update(table_name, only_inherits_parent=0, join_table='', join_type='', alias='', join_table_alias='',
           join_condition='', join_using_condition='', \
           update_set_clause={}, from_item: tuple = (), where_con='', order_bys='', limit_count='', returning='',
           returning_into_para=''):
    sql_clause_create_view = '''<%doc>Command:     UPDATE
Description: update rows of a table
Syntax:
</%doc>
    <%!
from random import random
def random_bool():
  return bool(round(random()))
%>
###############  [ WITH [ RECURSIVE ] with_query [, ...] ]未实现
  UPDATE 
%if only_inherits_parent:
    ONLY 
%endif
 ${table_name}  
%if alias:
    %if random_bool():
        AS 
    %endif
    ${alias}
%endif

%if join_table:
       ${join_type} JOIN ${join_table} 
    
    %if join_table_alias:
       %if random_bool():
            AS 
        %endif
        ${join_table_alias}
    %endif
    
    %if join_condition:
      ON ${join_condition}
    %endif
    
    %if join_using_condition:
      USING (${','.join(join_using_condition)})
    %endif
    
%endif    

%if update_set_clause:
      SET 
    %for i in range(len(update_set_clause.keys())-1):

        ${list(update_set_clause.keys())[i]}=${update_set_clause[list(update_set_clause.keys())[i]]},

    %endfor

    ${list(update_set_clause.keys())[-1]}=${update_set_clause[list(update_set_clause.keys())[-1]]}

%endif

%if from_item:
 from ${','.join(from_item)}
%endif

%if where_con:
WHERE ${where_con}
%endif

%if order_bys:
ORDER BY ${','.join(order_bys)}
%endif

%if limit_count:
LIMIT ${limit_count}
%endif

%if returning:
    %if 'return' in returning and returning['return']:
    RETURN 
    %else:
    RETURNING
    %endif 
    %if 'returning_items' in returning and returning['returning_items']:
    
        %for i in range(len(returning['returning_items'].keys())-1):
            %if 'alias' in returning['returning_items'].get(list(returning['returning_items'].keys())[i]) and  returning['returning_items'].get(list(returning['returning_items'].keys())[i]).get('alias'):
                ${list(returning['returning_items'].keys())[i]} 
                %if random_bool():
                AS  
                %endif
                ${returning['returning_items'].get(list(returning['returning_items'].keys())[i]).get('alias')} ,
            %else:
                 ${list(returning['returning_items'].keys())[i]},
            %endif
        %endfor  
        
         %if  hasattr(returning['returning_items'].get(list(returning['returning_items'].keys())[-1]),'alias') and  returning['returning_items'].get(list(returning['returning_items'].keys())[-1]).get('alias'):
                ${list(returning['returning_items'].keys())[i]} 
                %if random_bool():
                AS  
                %endif
                ${returning['returning_items'].get(list(returning['returning_items'].keys())[-1]).get('alias')} 
            %else:
                 ${list(returning['returning_items'].keys())[-1]}
            %endif
    %else:
    * 
    %endif  
      
    %if returning_into_para:
         into ${','.join(returning_into_para)}
    %endif
%endif


'''
    mylookup = TemplateLookup(directories=[r''])
    t = Template(sql_clause_create_view, lookup=mylookup)
    sql_statment = t.render(table_name=table_name, only_inherits_parent=only_inherits_parent, join_table=join_table,
                            join_type=join_type, alias=alias, join_table_alias=join_table_alias, \
                            join_condition=join_condition, join_using_condition=join_using_condition, \
                            update_set_clause=update_set_clause, from_item=from_item, where_con=where_con,
                            order_bys=order_bys, limit_count=limit_count, returning=returning,
                            returning_into_para=returning_into_para).replace('\n', '')
    sql_statment = re.sub('\s\s+', ' ', sql_statment) + ';'
    return sql_statment


# print(update(table_name='x',update_set_clause={'a':1,'(f,h)':'(select 1,2)'},from_item=('y',),returning={'returning_items':{'a':{'alias':1},'b':'1'}},returning_into_para=('x',)))


def select(tables: tuple, select_item: dict = {}, where_con='', group_bys: tuple = ()):
    sql_clause_create_view = '''<%doc>Command:     SELECT
Description: retrieve rows from a table or view
Syntax:
</%doc>
################[ WITH [ RECURSIVE ] with_query [, ...] ]未实现
################| [ WITH [ { PROCEDURE | FUNCTION } with_func_query [, ...] ] ]未实现
SELECT 
##################[ ALL | DISTINCT [ ON ( expression [, ...] ) ] ]未实现
##################    [ TOP n [, m | ( m ) ] ]未实现
##################    | [ TOP ( n ) [, m | ( m ) ] ]未实现
%if select_item:
    %for i in select_item:
        ${i} ${select_item[i]} 
    %endfor
$else:
    * 
%endif    
FROM   ${','.join(tables)}
########    [ SAMPLE [ BLOCK ] ( sample_count ) ] [ SEED ( seed_count ) ]未实现
  
%if where_con:
  WHERE  ${where_con} 
%endif 
##########    [ START WITH condition CONNECT BY [ NOCYCLE ] condition ]未实现
##########    | [ CONNECT BY [ NOCYCLE ] condition [ START WITH condition ] ]未实现
%if group_bys:
    GROUP BY  ${','.join(group_bys)}
%endif    [ GROUP BY grouping_element [, ...] ]                                       未实现
##############    [ HAVING condition [, ...] ]                                        未实现
##############    [ WINDOW window_name AS ( window_definition ) [, ...] ]             未实现
##############    [ { UNION | INTERSECT | EXCEPT | MINUS } [ ALL | DISTINCT ] select ]未实现
%if  order_bys:
    ORDER  BY    ${','.join(order_bys)}         
%endif
%if limit_count:
    LIMIT  ${limit_count} 
%endif
########    [ ORDER [ SIBLINGS ] BY expression [ ASC | DESC | USING operator ] [ NULLS { FIRST | LAST } ] [, ...] ] [ LIMIT { count | ALL } ]   部分实现
########    [ OFFSET start [ ROW | ROWS ] ]                                                                                                  未实现
########    [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ONLY ]                                                                         未实现
########    [ FOR { UPDATE | NO KEY UPDATE | SHARE | KEY SHARE } [ OF table_name [, ...] ] [ NOWAIT | SKIP LOCKED | WAIT wait_time] [...] ]  未实现

####################where from_item can be one of:                                                                                   未实现
####################                                                                                                                 未实现
####################    [ ONLY ] table_name [ * ] [ [ AS ] alias [ ( column_alias [, ...] ) ] ]                                      未实现
####################                [ TABLESAMPLE sampling_method ( argument [, ...] ) [ REPEATABLE ( seed ) ] ]                     未实现
####################    [ LATERAL ] ( select ) [ AS ] alias [ ( column_alias [, ...] ) ]                                             未实现
####################    with_query_name [ [ AS ] alias [ ( column_alias [, ...] ) ] ]                                                未实现
####################    [ LATERAL ] function_name ( [ argument [, ...] ] )                                                           未实现
####################                [ WITH ORDINALITY ] [ [ AS ] alias [ ( column_alias [, ...] ) ] ]                                未实现
####################    [ LATERAL ] function_name ( [ argument [, ...] ] ) [ AS ] alias ( column_definition [, ...] )                未实现
####################    [ LATERAL ] function_name ( [ argument [, ...] ] ) AS ( column_definition [, ...] )                          未实现
####################    [ LATERAL ] ROWS FROM( function_name ( [ argument [, ...] ] ) [ AS ( column_definition [, ...] ) ] [, ...] ) 未实现
####################                [ WITH ORDINALITY ] [ [ AS ] alias [ ( column_alias [, ...] ) ] ]                                未实现
####################    [ from_item PIVOT ( pivot_clause pivot_for_clause pivot_in_clause ) [ alias ] ]                              未实现
####################    [ from_item UNPIVOT [ { INCLUDE | EXCLUDE } NULLS ] ( unpivot_value_clause unpivot_for_clause unpivot_in_clause) [ [ AS ] alias [ ( column_alias [, ...] ) ] ] ] 未实现      
####################    from_item [ NATURAL ] join_type from_item [ ON join_condition | USING ( join_column [, ...] ) ]   未实现
####################                                                                                                      未实现
####################and pivot_clause is:                                                                                  未实现
####################    agg_func_expression ( expr ) [ [ AS ] output_name ] [, ...]                                       未实现
####################and pivot_for_clause is:                                                                              未实现
####################    FOR { pivot_column | ( pivot_column [, ...] ) }                                                   未实现
####################and pivot_in_clause is:                                                                               未实现
####################    IN ( [ const_expression [ [ AS ] output_name ] [, ...] ] )                                        未实现
####################and unpivot_value_clause is:                                                                          未实现
####################    { column_name | ( column_name [, ...] ) }                                                         未实现
####################and unpivot_for_clause is:                                                                            未实现
####################    FOR { output_column | ( output_column [, ...] ) }                                                 未实现
####################and unpivot_in_clause is:                                                                             未实现
####################    IN ( input_data_column [ AS constant ] [, ...] )                                                  未实现
####################                                                                                                      未实现
####################and grouping_element can be one of:                                                                   未实现
####################                                                                                                      未实现
####################    ( )                                                                                               未实现
####################    expression                                                                                        未实现
####################    ( expression [, ...] )                                                                            未实现
####################    ROLLUP ( { expression | ( expression [, ...] ) } [, ...] )                                        未实现
####################    CUBE ( { expression | ( expression [, ...] ) } [, ...] )                                          未实现
####################    GROUPING SETS ( grouping_element [, ...] )                                                        未实现
####################                                                                                                      未实现
####################and with_func_query is:                                                                               未实现
####################                                                                                                      未实现
####################    func_name ( [ [ argmode ] [ argname ] argtype [ { DEFAULT | = } default_expr ] [, ...] ] )        未实现
####################    [ RETURNS rettype                                                                                 未实现
####################      | RETURNS TABLE ( column_name column_type [, ...] ) ]                                           未实现
####################    { LANGUAGE lang_name                                                                              未实现
####################        | TRANSFORM { FOR TYPE type_name } [, ... ]                                                   未实现
####################        | WINDOW                                                                                      未实现
####################        | IMMUTABLE | STABLE | VOLATILE | [ NOT ] LEAKPROOF                                           未实现
####################        | CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT                                  未实现
####################        | [ EXTERNAL ] SECURITY INVOKER | [ EXTERNAL ] SECURITY DEFINER                               未实现
####################        | PARALLEL { UNSAFE | RESTRICTED | SAFE }                                                     未实现
####################        | COST execution_cost                                                                         未实现
####################        | ROWS result_rows                                                                            未实现
####################        | SUPPORT support_function                                                                    未实现
####################        | SET configuration_parameter { TO value | = value | FROM CURRENT }                           未实现
####################        | { AS | IS } 'definition'                                                                    未实现
####################        | { AS | IS } 'obj_file', 'link_symbol'                                                       未实现
####################    }                                                                                                 未实现
####################                                                                                                      未实现
####################and with_query is:                                                                                    未实现
####################                                                                                                      未实现
####################    with_query_name [ ( column_name [, ...] ) ] AS [ [ NOT ] MATERIALIZED ] ( select | values | insert | update | delete )                      未实现
####################        [ SEARCH { BREADTH | DEPTH } FIRST BY column_name [, ...] SET search_seq_col_name ]                                                     未实现
####################        [ CYCLE column_name [, ...] SET cycle_mark_col_name [ TO cycle_mark_value DEFAULT cycle_mark_default ] USING cycle_path_col_name ]      未实现
####################                                                                                                                                                未实现
####################TABLE [ ONLY ] table_name [ * ]                                                                                                                 未实现
'''
    mylookup = TemplateLookup(directories=[r''])
    t = Template(sql_clause_create_view, lookup=mylookup)
    sql_statment = t.render(tables=tables, select_item=select_item, where_con=where_con, group_bys=group_bys).replace(
        '\n', '')
    sql_statment = re.sub('\s\s+', ' ', sql_statment) + ';'
    return sql_statment


def set_para(parameter, value='DEFAULT'):
    sql_clause_create_view = '''<%!
from random import random
def random_bool():
  return bool(round(random()))
%>
<%doc>Command:     SET
Description: change a run-time parameter
Syntax:
</%doc>
SET 
###################[ SESSION | LOCAL ] 未实现
${parameter} 
%if random_bool():
    TO
%else:
    =
%endif

%if value:
    ${value}
%endif 

############SET [ SESSION | LOCAL ] TIME ZONE { timezone | LOCAL | DEFAULT } 未实现
'''
    mylookup = TemplateLookup(directories=[r''])
    t = Template(sql_clause_create_view, lookup=mylookup)
    sql_statment = t.render(parameter=parameter, value=value).replace('\n', '')
    sql_statment = re.sub('\s\s+', ' ', sql_statment) + ';'
    return sql_statment


def create_extension(extension_name, ifnotexist=1):
    sql_clause_create_view = '''<%doc>Command:     CREATE EXTENSION
Description: install an extension
Syntax:
</%doc>
CREATE EXTENSION 
%if ifnotexist:
  IF NOT EXISTS   
%endif
 ${extension_name}
############    [ WITH ] [ SCHEMA schema_name ]    未实现
############             [ VERSION version ]       未实现
############             [ FROM old_version ]      未实现
############             [ CASCADE ]               未实现
    '''
    mylookup = TemplateLookup(directories=[r''])
    t = Template(sql_clause_create_view, lookup=mylookup)
    sql_statment = t.render(extension_name=extension_name, ifnotexist=ifnotexist).replace('\n', '')
    sql_statment = re.sub('\s\s+', ' ', sql_statment) + ';'
    return sql_statment


def drop_extension(extension_names, ifexist=1, cascade=1, restrict=0):
    sql_clause_create_view = '''<%doc>Command:     DROP EXTENSION
Description: remove an extension
Syntax:
</%doc>
DROP EXTENSION 
%if ifnotexist:
  IF EXISTS   
%endif
 ${','.join(extension_names)}  

%if cascade:
  CASCADE   
%endif  

%if not cascade and restrict:
  RESTRICT   
%endif  
    '''
    mylookup = TemplateLookup(directories=[r''])
    t = Template(sql_clause_create_view, lookup=mylookup)
    sql_statment = t.render(extension_names=extension_names, ifexist=ifexist, cascade=cascade,
                            restrict=restrict).replace('\n', '')
    sql_statment = re.sub('\s\s+', ' ', sql_statment) + ';'
    return sql_statment


def anonymous_block(declare_paras: dict = {}, pre_define_proc='', body='null;'):
    assert reduce(lambda x, y: x and y, declare_paras.keys())
    assert reduce(lambda x, y: x and y, declare_paras.values())
    declare_paras = ['%s :%s' % (key, value) for key, value in declare_paras.items()]
    sql_clause_create_view = '''<%doc>{declare [...]}
begin
[body...]
end;
/ 
</%doc>
--specialsql
%if declare_paras:
declare 
%for i in declare_paras:
${i}; 
%endfor
%endif
${pre_define_proc} 
begin
${body}
end;       
/
--specialsql
        '''
    mylookup = TemplateLookup(directories=[r''])
    t = Template(sql_clause_create_view, lookup=mylookup)
    sql_statment = t.render(declare_paras=declare_paras, pre_define_proc=pre_define_proc, body=body).replace('\n\n',
                                                                                                             '\n')
    # sql_statment = re.sub('\s\s\s+', ' ', sql_statment) + ';'
    return sql_statment


def test_case_before(*args):
    case = '''【前置条件】清理环境:
%s
''' % '\n'.join(args)
    return case


def test_case_after(*args):
    case = '''
【后置条件】清理环境:
%s
''' % '\n'.join(args)
    return case


def test_case_body(step: dict):
    case = '''<%count=0%>
%for i in step:
【步骤${count+1}】${i}:<%count+=1%>
${step[i]}
%endfor
'''
    t = Template(case)
    case_statment = t.render(step=step).replace('\n\n', '\n').strip()
    return case_statment


def block_yinhaochuli(statment: str):
    return statment.replace("'", "''");


def test_case():
    count = 0
    table_name = 'UXDBC_Oracle_Plsql_DynamicReturningClause_%04d_tb01' % (count + 1)
    column = {'id': {'datatype': 'int'}, 'name': {'datatype': 'varchar(200)'}}
    declare_paras = {'v_block': 'int', 'y': 'int'}
    do_body = 'v_block:=' + "'" + block_yinhaochuli(
        update(table_name=table_name, join_table="(select 1,'李四') from dual", join_table_alias='y',
               join_using_condition='id', returning={1: 2}, returning_into_para=(':1',))) + "'"
    exec1 = 'execute immediate v_block into y;'
    body = '\n'.join((do_body, exec1))
    print(body)
    test_case1 = test_case_before(create_extension(extension_name='plsql'),
                                  create_extension(extension_name='orafce'),
                                  create_extension(extension_name='UX_IMPLICIT'),
                                  drop_table(table_name=(table_name,)))
    test_case2 = test_case_body({
        '创建普通表': create_table(table_name=table_name, column=column),
        '插入数据': insert(table_name=table_name, values=(1, '张三')) + insert(table_name=table_name,
                                                                               values=(2, '李四')),
        '执行动态sql': anonymous_block(declare_paras=declare_paras, body='\n'.join((do_body, exec1))),
        '查询表': select(tables=(table_name,))})
    test_case3 = test_case_after(drop_table(table_name=(table_name,)))
    return test_case1 + test_case2 + test_case3


class ast_Node:
    def __init__(self, token, next=None, node_type='KEYWORDS', parent=None, ismuststartwith: bool = False):
        self.next = next
        self.token = token
        self.parent = parent
        self.node_type = node_type
        # self.ismuststartwith=ismuststartwith


# def ast_gen_sql(ast :str,**paras):
#     stack1=[]
#     stack2=[]
#     pingxingnodes=[]
#     ast=ast.strip()
#
#     for i in range(len(ast)):
#         if ast[i]=='[':
#             stack1.append('[')
#             continue
#         if ast[i]==']':
#             stack1.pop()
#             parent.next=pingxingnodes
#
#
#         token=''
#         newstr=ast[i:]
#         start=0
#         point=i
#         for j in range(point,len(newstr)):
#             if re.match(r'[\w,\,,\*]',newstr[j]):
#                 token=token+newstr[j]
#                 start=1
#             if start and newstr[j]==' ':
#                 point=j
#                 break
#         parent=ast_Node(token)
#         i=point;


# ast=ast.strip().replace('[',' [ ').replace(']',' ] ')
# source =ast.split(' ')


class node():
    def __init__(self):
        self.next = []
        self.val = []
        self.parent = None


def ast_gen_sql(source):
    lenth = len(source)
    i = 0
    temp_node = node()
    temp_node.val = []
    templist = []
    while i < lenth:
        # if temp_node.parent:
        #     print(temp_node.parent.val)
        # print(temp_node.val)
        if source[i] == '[':
            if templist:
                temp_node.val.append(templist)

            j = i
            templist = []
            while j < lenth:
                j += 1
                if source[j] == '[' or source[j] == ']':
                    break
                else:
                    templist.append(source[j])
            child = node()
            child.val.append(templist)
            child.parent = temp_node
            temp_node.next.append(child)
            temp_node = child

            templist = []
            # print(temp_node.val)
            i = j
            continue

        if source[i] == ']':
            j = i
            templist = []
            while 0 <= j:
                j -= 1
                if source[j] == '[' or source[j] == ']':
                    break
                else:
                    templist.append(source[j])
            if len(templist) > 1:
                templist.reverse()
            if templist == temp_node.val[-1]:
                templist = []
            temp_node.parent.val.append(len(temp_node.parent.next) - 1)
            temp_node.val.append(templist)
            # print(temp_node.val)
            temp_node = temp_node.parent
            templist = []
            i += 1
            continue
        templist.append(source[i])
        if i == lenth - 1:
            temp_node.val.append(templist)
        # print(templist)
        i += 1
        continue
    # print(temp_node.val)
    return temp_node


def preorder(root):  ###########查看树工具
    print(root.val)
    if not root.next:
        return
    for child in root.next:
        preorder(child)


def generate_sql(root_node):  ######树路径遍历 +很难

    length = len(root_node.val)
    local_temp = []
    i = 0
    flag = False
    while i < length:
        if isinstance(root_node.val[i], list):
            # local_temp.append(root_node.val[i])
            if not root_node.val[i] or local_temp == []:
                local_temp.append(root_node.val[i])
            else:

                for m in local_temp:
                    m += root_node.val[i]
            # if not flag :
            #     local_temp.append(root_node.val[i])
            # else:
            #     for m in local_temp:
            #         m+=root_node.val[i]


        else:
            temp1 = []
            for k in generate_sql(root_node.next[root_node.val[i]]):
                # yield k ,i,local_temp,temp1
                if local_temp == []:
                    temp1.append(k)
                else:
                    for l in local_temp:
                        temp1.append(l + k)
                        if not l:
                            flag = True

            local_temp = temp1

        # print(local_temp)
        # if isinstance(root_node.val[i],list) and not root_node.val[i]:
        #     i=length-1
        # else:
        #     i+=1
        i += 1

    if flag:
        local_temp.append([])
    if not root_node.next:
        return local_temp
    else:
        return local_temp


def func1(ast):
    l = ast.replace('[', ' [ ').replace(']', ' ] ').replace('\n', ' ').split(' ')
    temp = []
    for i in l:
        if i:
            temp.append(i)
    s = ast_gen_sql(temp)
    print(11111111111111111, s.val)
    return s


if __name__ == '__main__':
    # print(func1(" [ from_item UNPIVOT [ { INCLUDE | EXCLUDE } NULLS ] ( unpivot_value_clause unpivot_for_clause unpivot_in_clause) [ [ AS ] alias [ ( column_alias [ , ... ] ) ] ] ]"))
    # print(func1("[ FOR { UPDATE | NO KEY UPDATE | SHARE | KEY SHARE } [ OF table_name [ , ... ] ] [ NOWAIT | SKIP LOCKED | WAIT wait_time ] [ ... ] ]"))
    # str='''DELETE [ FROM ] [ ONLY ] table_name [ * ] [ [ AS ] alias ]
    # [ USING from_item [, ...] ]
    # [ WHERE condition | WHERE CURRENT OF cursor_name ]
    # [ ORDER BY column_name [ LIMIT limit_count ] ]
    # [ RETURNING * | output_expression [ [ AS ] output_name ] [, ...] ]'''
    # str='''DELETE [ FROM ] table_name [ RETURNING * | output_expression [ [ AS ] output_name ] [, ...] ] '''
    # print(func1(str))
    str = '''CREATE [ [ GLOBAL | LOCAL ] { TEMPORARY | TEMP } | UNLOGGED ] TABLE  '''
    print(preorder(func1(str)))
    s = generate_sql(func1(str))
    for i in s:
        print(i)
        print(' '.join(i) + ' ;')
    print(len(s))



