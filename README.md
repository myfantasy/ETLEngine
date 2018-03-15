# ETLEngine

## rules.json

#### rule_name
��� �������

#### repeat_timeout
���-�� ����������� ����� ������� ����� ��������� ������

#### query
������, ������� ����� ���������

#### src_type
��� ��������� 
"ms" -- Microsoft SqlServer �� ��� ��������� conn_settings, 
"pg" -- PostgreSql �� ��� ��������� conn_settings, 
"mc" -- ������ �� ��� ��������� conn_settings

"ms_direct" -- Microsoft SqlServer ������ ����������� �����������, 
"pg_direct" -- PostgreSql ������ ����������� �����������, 
"mc_direct" -- ������ URL �����������

#### src_name
��� ��������� �� (conn_settings) / ������ ���������� / URL �������

#### dst_type
��� ���� (Relf ���� ���������� ������)

#### dst_name
��� ���� �� (conn_settings) / ������ ���������� / URL �������

#### src_id_name
��� ������� � ��������������� �������� "_id"

#### dst_ready_flag_name
��� ����� ��������� �������� "_is_ready"


#### src_table
������� ��������

#### src_complite_proc
������ �� ��������� ��������� ��������� (��������, ��� �� �� ������ ����������� �������� �������� ��� ��� ���������)



#### dst_table
������� ����

#### dst_prepare_complite_proc
������ �� ��������������� ��������� �� ���� (��������, ��� �� �������� ������ ��������������� ����������� ��������)

#### dst_complite_proc
������ �� ��������� ��������� �� ���� (��������, ��� �� �� ������ ����������� �������� �������� ��� ��� ���������)

#### limit
����� ������, �������� �������������� 1 ���
default 1000
#### timeout
������� � �������� �� ���������� ��������
default 10


#### error_url
URL �� ������� � ���� JSON ������������ ��������� ��������� ������� � �������

#### complite_url
URL �� ������� � ���� JSON ������������ ��������� �������� ��������� �������
