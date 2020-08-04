
# parsetab.py
# This file is automatically generated. Do not edit.
# pylint: disable=W,C,R
_tabversion = '3.10'

_lr_method = 'LALR'

_lr_signature = 'ALL AND ASC BOOLEAN BOUND BY BYTE COLON COMA CONSTANT CONTAINS DATATYPE DATETIME DECIMAL DESC DISTINCT DIV DOUBLE EQUALS FILTER FLOAT GREATER GREATEREQ ID INT INTEGER ISBLANK ISIRI ISLITERAL ISURI LANG LANGMATCHES LCASE LESS LESSEQ LIMIT LKEY LONG LPAR MINUS NEG NEGATIVEINT NEQUALS NONNEGINT NONPOSINT NUMBER OFFSET OPTIONAL OR ORDER PLUS POINT POSITIVEINT PREFIX REGEX RKEY RPAR SAMETERM SELECT SHORT STR STRING TIMES UCASE UNION UNSIGNEDBYTE UNSIGNEDINT UNSIGNEDLONG UNSIGNEDSHORT UPPERCASE URI VARIABLE WHERE\n    parse_sparql : prefix_list query order_by limit offset\n    \n    parse_sparql : prefix_list query order_by offset limit\n    \n    prefix_list : prefix prefix_list\n    \n    prefix_list : empty\n    \n    empty :\n    \n    prefix : PREFIX uri\n    \n    uri : ID COLON ID\n    \n    uri : ID COLON URI\n    \n    uri : URI\n    \n    order_by : empty\n    \n    var_order_list : empty\n    \n    limit : LIMIT NUMBER\n    \n    limit : empty\n    \n    offset : OFFSET NUMBER\n    \n    offset : empty\n    \n    query : SELECT distinct var_list WHERE group_graph_pattern\n    \n    query : SELECT distinct ALL WHERE group_graph_pattern\n    \n    distinct : DISTINCT\n    \n    distinct : empty\n    \n    group_graph_pattern : LKEY group_graph_pattern RKEY\n    \n    group_graph_pattern :  LKEY group_graph_pattern RKEY POINT LKEY group_graph_pattern RKEY\n    \n    group_graph_pattern : LKEY group_graph_pattern RKEY UNION LKEY group_graph_pattern RKEY\n    \n    group_graph_pattern : LKEY group_graph_pattern RKEY OPTIONAL LKEY group_graph_pattern RKEY\n    \n    group_graph_pattern : group_graph_pattern OPTIONAL LKEY group_graph_pattern RKEY\n    \n    group_graph_pattern : triples_block\n    \n    group_graph_pattern : empty\n    \n    triples_block : triple POINT triples_block\n    \n    triples_block : triple\n    \n    triples_block : empty\n    \n    var_list : var_list VARIABLE\n    \n    var_list : VARIABLE\n    \n    triple : subject predicate object\n    \n    predicate : ID\n    \n    predicate : uri\n    \n    predicate : VARIABLE\n    \n    subject : uri\n    \n    subject : VARIABLE\n    \n    object : uri\n    \n    object : VARIABLE\n    \n    object : CONSTANT\n    '
    
_lr_action_items = {'ALL':([11,15,16,17,],[-5,25,-18,-19,]),'CONSTANT':([7,18,19,49,50,51,52,],[-9,-7,-8,57,-34,-35,-33,]),'LKEY':([34,36,38,47,54,61,62,63,65,66,67,],[38,38,38,54,38,65,66,67,38,38,38,]),'POINT':([7,18,19,42,53,57,58,59,60,],[-9,-7,-8,48,61,-40,-32,-38,-39,]),'DISTINCT':([11,],[16,]),'WHERE':([25,26,27,35,],[34,-31,36,-30,]),'RKEY':([7,18,19,37,38,41,42,46,48,53,54,55,56,57,58,59,60,64,65,66,67,68,69,70,71,72,73,74,],[-9,-7,-8,-25,-5,-26,-28,53,-5,-20,-5,-27,-29,-40,-32,-38,-39,68,-5,-5,-5,-24,72,73,74,-21,-22,-23,]),'URI':([1,7,12,18,19,34,36,38,39,43,44,48,49,50,51,52,54,65,66,67,],[7,-9,19,-7,-8,7,7,7,-36,-37,7,7,7,-34,-35,-33,7,7,7,7,]),'NUMBER':([20,21,],[28,29,]),'UNION':([53,],[62,]),'PREFIX':([0,3,6,7,18,19,],[1,1,-6,-9,-7,-8,]),'LIMIT':([7,10,13,14,18,19,23,24,28,34,36,37,40,41,42,45,48,53,55,56,57,58,59,60,68,72,73,74,],[-9,-5,21,-10,-7,-8,21,-15,-14,-5,-5,-25,-17,-26,-28,-16,-5,-20,-27,-29,-40,-32,-38,-39,-24,-21,-22,-23,]),'OFFSET':([7,10,13,14,18,19,22,24,29,34,36,37,40,41,42,45,48,53,55,56,57,58,59,60,68,72,73,74,],[-9,-5,20,-10,-7,-8,20,-13,-12,-5,-5,-25,-17,-26,-28,-16,-5,-20,-27,-29,-40,-32,-38,-39,-24,-21,-22,-23,]),'VARIABLE':([7,11,15,16,17,18,19,26,27,34,35,36,38,39,43,44,48,49,50,51,52,54,65,66,67,],[-9,-5,26,-18,-19,-7,-8,-31,35,43,-30,43,43,-36,-37,51,43,60,-34,-35,-33,43,43,43,43,]),'COLON':([8,52,],[12,12,]),'OPTIONAL':([7,18,19,34,36,37,38,40,41,42,45,46,48,53,54,55,56,57,58,59,60,64,65,66,67,68,69,70,71,72,73,74,],[-9,-7,-8,-5,-5,-25,-5,47,-26,-28,47,47,-5,63,-5,-27,-29,-40,-32,-38,-39,47,-5,-5,-5,-24,47,47,47,-21,-22,-23,]),'ID':([1,7,12,18,19,34,36,38,39,43,44,48,49,50,51,52,54,65,66,67,],[8,-9,18,-7,-8,8,8,8,-36,-37,52,8,8,-34,-35,-33,8,8,8,8,]),'SELECT':([0,3,4,5,6,7,9,18,19,],[-5,-5,11,-4,-6,-9,-3,-7,-8,]),'$end':([2,7,10,13,14,18,19,22,23,24,28,29,30,31,32,33,34,36,37,40,41,42,45,48,53,55,56,57,58,59,60,68,72,73,74,],[0,-9,-5,-5,-10,-7,-8,-5,-5,-13,-14,-12,-1,-15,-2,-13,-5,-5,-25,-17,-26,-28,-16,-5,-20,-27,-29,-40,-32,-38,-39,-24,-21,-22,-23,]),}

_lr_action = {}
for _k, _v in _lr_action_items.items():
   for _x,_y in zip(_v[0],_v[1]):
      if not _x in _lr_action:  _lr_action[_x] = {}
      _lr_action[_x][_k] = _y
del _lr_action_items

_lr_goto_items = {'triples_block':([34,36,38,48,54,65,66,67,],[37,37,37,55,37,37,37,37,]),'predicate':([44,],[49,]),'order_by':([10,],[13,]),'distinct':([11,],[15,]),'object':([49,],[58,]),'uri':([1,34,36,38,44,48,49,54,65,66,67,],[6,39,39,39,50,39,59,39,39,39,39,]),'parse_sparql':([0,],[2,]),'group_graph_pattern':([34,36,38,54,65,66,67,],[40,45,46,64,69,70,71,]),'prefix':([0,3,],[3,3,]),'limit':([13,23,],[22,32,]),'triple':([34,36,38,48,54,65,66,67,],[42,42,42,42,42,42,42,42,]),'offset':([13,22,],[23,30,]),'query':([4,],[10,]),'var_list':([15,],[27,]),'prefix_list':([0,3,],[4,9,]),'empty':([0,3,10,11,13,22,23,34,36,38,48,54,65,66,67,],[5,5,14,17,24,31,33,41,41,41,56,41,41,41,41,]),'subject':([34,36,38,48,54,65,66,67,],[44,44,44,44,44,44,44,44,]),}

_lr_goto = {}
for _k, _v in _lr_goto_items.items():
   for _x, _y in zip(_v[0], _v[1]):
       if not _x in _lr_goto: _lr_goto[_x] = {}
       _lr_goto[_x][_k] = _y
del _lr_goto_items
_lr_productions = [
  ("S' -> parse_sparql","S'",1,None,None,None),
  ('parse_sparql -> prefix_list query order_by limit offset','parse_sparql',5,'p_parse_sparql_0','sparqlparser.py',104),
  ('parse_sparql -> prefix_list query order_by offset limit','parse_sparql',5,'p_parse_sparql_1','sparqlparser.py',112),
  ('prefix_list -> prefix prefix_list','prefix_list',2,'p_prefix_list','sparqlparser.py',120),
  ('prefix_list -> empty','prefix_list',1,'p_empty_prefix_list','sparqlparser.py',127),
  ('empty -> <empty>','empty',0,'p_empty','sparqlparser.py',134),
  ('prefix -> PREFIX uri','prefix',2,'p_prefix','sparqlparser.py',141),
  ('uri -> ID COLON ID','uri',3,'p_uri_0','sparqlparser.py',148),
  ('uri -> ID COLON URI','uri',3,'p_uri_1','sparqlparser.py',155),
  ('uri -> URI','uri',1,'p_uri_2','sparqlparser.py',162),
  ('order_by -> empty','order_by',1,'p_order_by_1','sparqlparser.py',176),
  ('var_order_list -> empty','var_order_list',1,'p_var_order_list_0','sparqlparser.py',183),
  ('limit -> LIMIT NUMBER','limit',2,'p_limit_0','sparqlparser.py',226),
  ('limit -> empty','limit',1,'p_limit_1','sparqlparser.py',233),
  ('offset -> OFFSET NUMBER','offset',2,'p_offset_0','sparqlparser.py',240),
  ('offset -> empty','offset',1,'p_offset_1','sparqlparser.py',247),
  ('query -> SELECT distinct var_list WHERE group_graph_pattern','query',5,'p_query_0','sparqlparser.py',254),
  ('query -> SELECT distinct ALL WHERE group_graph_pattern','query',5,'p_query_1','sparqlparser.py',261),
  ('distinct -> DISTINCT','distinct',1,'p_distinct_0','sparqlparser.py',268),
  ('distinct -> empty','distinct',1,'p_distinct_1','sparqlparser.py',275),
  ('group_graph_pattern -> LKEY group_graph_pattern RKEY','group_graph_pattern',3,'p_ggp_0','sparqlparser.py',283),
  ('group_graph_pattern -> LKEY group_graph_pattern RKEY POINT LKEY group_graph_pattern RKEY','group_graph_pattern',7,'p_ggp_1','sparqlparser.py',296),
  ('group_graph_pattern -> LKEY group_graph_pattern RKEY UNION LKEY group_graph_pattern RKEY','group_graph_pattern',7,'p_ggp_2','sparqlparser.py',304),
  ('group_graph_pattern -> LKEY group_graph_pattern RKEY OPTIONAL LKEY group_graph_pattern RKEY','group_graph_pattern',7,'p_ggp_3','sparqlparser.py',311),
  ('group_graph_pattern -> group_graph_pattern OPTIONAL LKEY group_graph_pattern RKEY','group_graph_pattern',5,'p_ggp_4','sparqlparser.py',318),
  ('group_graph_pattern -> triples_block','group_graph_pattern',1,'p_ggp_5','sparqlparser.py',325),
  ('group_graph_pattern -> empty','group_graph_pattern',1,'p_ggp_6','sparqlparser.py',332),
  ('triples_block -> triple POINT triples_block','triples_block',3,'p_triples_block_2','sparqlparser.py',339),
  ('triples_block -> triple','triples_block',1,'p_triples_block_1','sparqlparser.py',346),
  ('triples_block -> empty','triples_block',1,'p_triples_block_0','sparqlparser.py',353),
  ('var_list -> var_list VARIABLE','var_list',2,'p_var_list','sparqlparser.py',866),
  ('var_list -> VARIABLE','var_list',1,'p_single_var_list','sparqlparser.py',873),
  ('triple -> subject predicate object','triple',3,'p_triple_0','sparqlparser.py',880),
  ('predicate -> ID','predicate',1,'p_predicate_rdftype','sparqlparser.py',887),
  ('predicate -> uri','predicate',1,'p_predicate_uri','sparqlparser.py',901),
  ('predicate -> VARIABLE','predicate',1,'p_predicate_var','sparqlparser.py',908),
  ('subject -> uri','subject',1,'p_subject_uri','sparqlparser.py',915),
  ('subject -> VARIABLE','subject',1,'p_subject_variable','sparqlparser.py',922),
  ('object -> uri','object',1,'p_object_uri','sparqlparser.py',929),
  ('object -> VARIABLE','object',1,'p_object_variable','sparqlparser.py',936),
  ('object -> CONSTANT','object',1,'p_object_constant','sparqlparser.py',943),
]
