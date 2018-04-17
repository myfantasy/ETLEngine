using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Http;
using System.Text;

namespace MyFantasy.ETLEngine.Common
{
    public static class CopyTable
    {
        public static bool CopyToTable(Rule r, bool set_complete = true)
        {
            var src_type = r.SrcType;
            var src_name = r.SrcName;

            var dst_type = r.DstType;
            var dst_name = r.DstName;

            var src_id_name = r.SrcIDName;

            var src_query = r.SrcQuery ?? "select * from " + r.SrcTable + (src_id_name.IsNullOrWhiteSpace() ? "" : " order by " + src_id_name) + ";";
            var dst_table = r.DstTable;

            var timeout = r.Timeout;
            var limit = r.Limit;


            //string src_conn_string, string dst_conn_string, string src_query, string dst_table, int limit, int timeout, out Exception ex


            Func<List<Tuple<Type, string>>, List<Dictionary<string, object>>, Tuple<bool, Exception>> write_func = null;

            if (dst_type == "ms")
            {
                write_func = (f, d) =>
                {
                    if (dst_table.Execute_Bulk(dst_name, out var res_in, f, d, timeout))
                    {
                        return new Tuple<bool, Exception>(true, null);
                    }
                    else
                    {
                        r.Error("Запись в целевую таблицу ms", res_in.e);
                        return new Tuple<bool, Exception>(false, res_in.e);
                    }
                };
            }
            else if (dst_type == "ms_direct")
            {
                write_func = (f, d) =>
                {
                    if (dst_table.Execute_Bulk(out var res_in, dst_name, f, d, timeout))
                    {
                        return new Tuple<bool, Exception>(true, null);
                    }
                    else
                    {
                        r.Error("Запись в целевую таблицу ms_direct", res_in.e);
                        return new Tuple<bool, Exception>(false, res_in.e);
                    }
                };
            }
            else if (dst_type == "pg")
            {
                write_func = (f, d) =>
                {
                    var e = QueryHandlerPg.Copy(d, f, dst_table, dst_name);
                    if (e == null)
                    {
                        return new Tuple<bool, Exception>(true, null);
                    }
                    else
                    {
                        r.Error("Запись в целевую таблицу pg", e);
                        return new Tuple<bool, Exception>(false, e);
                    }
                };

            }
            else if (dst_type == "pg_direct")
            {
                write_func = (f, d) =>
                {
                    var e = QueryHandlerPg.Copy(dst_name, d, f, dst_table);
                    if (e == null)
                    {
                        return new Tuple<bool, Exception>(true, null);
                    }
                    else
                    {
                        r.Error("Запись в целевую таблицу pg_direct", e);
                        return new Tuple<bool, Exception>(false, e);
                    }
                };
            }


            if (src_type == "ms")
            {
                if (src_query.Execute_Step(src_name, out var res, limit,
                        write_func, timeout))
                {
                    if (set_complete)
                        r.Complete();
                    return true;
                }
                else
                {
                    r.Error("Получение данных из источника ms", res.e);
                }
            }
            else if (src_type == "ms_direct")
            {
                if (src_query.Execute_Step(out var res, src_name, limit,
                        write_func, timeout))
                {
                    if (set_complete)
                        r.Complete();
                    return true;
                }
                else
                {
                    r.Error("Получение данных из источника ms_direct", res.e);
                }
            }
            else if (src_type == "pg")
            {
                if (src_query.ExecutePg_Step(src_name, out var res, limit,
                        write_func, timeout))
                {
                    if (set_complete)
                        r.Complete();
                    return true;
                }
                else
                {
                    r.Error("Получение данных из источника pg", res.e);
                }
            }
            else if (src_type == "pg_direct")
            {
                if (src_query.ExecutePg_Step(out var res, src_name, limit,
                        write_func, timeout))
                {
                    if (set_complete)
                        r.Complete();
                    return true;
                }
                else
                {
                    r.Error("Получение данных из источника pg_direct", res.e);
                }
            }
            
            return false;
        }


        public static bool QueueLoadComplete(Rule r)
        {
            var src_type = r.SrcType;
            var src_name = r.SrcName;

            var dst_type = r.DstType;
            var dst_name = r.DstName;

            var src_id_name = r.SrcIDName.TryParseOrDefault("_id");

            var dst_ready_flag_name = r.DstReadyFlagName.TryParseOrDefault("_is_ready");

            var src_table = r.SrcTable;
            var dst_table = r.DstTable;

            var limit = r.Limit;
            var timeout = r.Timeout;

            var dst_prepare_complete_proc = r.DstPrepareCompleteProc;
            var src_complete_proc = r.SrcCompleteProc;
            var dst_complete_proc = r.DstCompleteProc;

            List<object> ids = new List<object>();

            bool need_complete = true;

            while (need_complete)
            {
                if (dst_type == "ms")
                {
                    string query = dst_prepare_complete_proc ?? "select top(" + limit.ConvertToDB() + ") " + src_id_name + " from " + dst_table + " where " + dst_ready_flag_name + " = 0 order by " + src_id_name + ";";
                    if (query.Execute(dst_name, out var res, timeout))
                    {
                        ids = res.res[0].Select(f => f.GetElement(src_id_name)).ToList();
                    }
                    else
                    {
                        r.Error("Получение данных из целевой таблицы для удаления ms", res.e);
                        return false;
                    }
                }
                else if (dst_type == "ms_direct")
                {
                    string query = dst_prepare_complete_proc ?? "select top(" + limit.ConvertToDB() + ") " + src_id_name + " from " + dst_table + " where " + dst_ready_flag_name + " = 0 order by " + src_id_name + ";";
                    if (query.Execute(out var res, dst_name, timeout))
                    {
                        ids = res.res[0].Select(f => f.GetElement(src_id_name)).ToList();
                    }
                    else
                    {
                        r.Error("Получение данных из целевой таблицы для удаления ms_direct", res.e);
                        return false;
                    }

                }
                else if (dst_type == "pg")
                {
                    string query = dst_prepare_complete_proc ?? "select " + src_id_name + " from " + dst_table + " where " + dst_ready_flag_name + " = false order by " + src_id_name + " limit " + limit.ConvertToDB() + ";";
                    if (query.ExecutePg(dst_name, out var res, timeout))
                    {
                        ids = res.res[0].Select(f => f.GetElement(src_id_name)).ToList();
                    }
                    else
                    {
                        r.Error("Получение данных из целевой таблицы для удаления pg", res.e);
                        return false;
                    }
                }
                else if (dst_type == "pg_direct")
                {
                    string query = dst_prepare_complete_proc ?? "select " + src_id_name + " from " + dst_table + " where " + dst_ready_flag_name + " = false order by " + src_id_name + " limit " + limit.ConvertToDB() + ";";
                    if (query.ExecutePg(out var res, dst_name, timeout))
                    {
                        ids = res.res[0].Select(f => f.GetElement(src_id_name)).ToList();
                    }
                    else
                    {
                        r.Error("Получение данных из целевой таблицы для удаления pg_direct", res.e);
                        return false;
                    }
                }


                if (ids.Any())
                {

                    if (src_type == "ms")
                    {
                        string src_complete_proc_do = src_complete_proc?.Replace("{values}", ids.ConvertToDB());
                        string query = src_complete_proc_do ?? "delete t from " + src_table + " t inner join ("+ ids.ConvertToDB() + ")s("+ src_id_name + ") on t."+ src_id_name + " = s." + src_id_name + ";";
                        if (query.Execute(src_name, out var res, timeout))
                        {
                        }
                        else
                        {
                            r.Error("Удаление из очереди ms", res.e);
                            return false;
                        }
                    }
                    else if (src_type == "ms_direct")
                    {
                        string src_complete_proc_do = src_complete_proc?.Replace("{values}", ids.ConvertToDB());
                        string query = src_complete_proc_do ?? "delete t from " + src_table + " t inner join (" + ids.ConvertToDB() + ")s(" + src_id_name + ") on t." + src_id_name + " = s." + src_id_name + ";";
                        if (query.Execute(out var res, src_name, timeout))
                        {
                        }
                        else
                        {
                            r.Error("Удаление из очереди ms_direct", res.e);
                            return false;
                        }
                    }
                    else if (src_type == "pg")
                    {
                        string src_complete_proc_do = src_complete_proc?.Replace("{array}", ids.ConvertToDB(FieldHandler.DbTypes.PG));
                        string query = src_complete_proc_do ?? "delete from " + src_table + " where " + src_id_name +" = any("+ ids.ConvertToDB() + ");";
                        if (query.ExecutePg(src_name, out var res, timeout))
                        {
                        }
                        else
                        {
                            r.Error("Удаление из очереди pg", res.e);
                            return false;
                        }
                    }
                    else if (src_type == "pg_direct")
                    {
                        string src_complete_proc_do = src_complete_proc?.Replace("{array}", ids.ConvertToDB(FieldHandler.DbTypes.PG));
                        string query = src_complete_proc_do ?? "delete from " + src_table + " where " + src_id_name + " = any(" + ids.ConvertToDB() + ");";
                        if (query.ExecutePg(out var res, src_name, timeout))
                        {
                        }
                        else
                        {
                            r.Error("Удаление из очереди pg_direct", res.e);
                            return false;
                        }
                    }




                    if (dst_type == "ms")
                    {
                        string dst_complete_proc_do = dst_complete_proc?.Replace("{values}", ids.ConvertToDB());
                        string query = dst_complete_proc_do ?? "update t set "+ dst_ready_flag_name + " = 1 from " + dst_table + " t inner join (" + ids.ConvertToDB() + ")s(" + src_id_name + ") on t." + src_id_name + " = s." + src_id_name + ";";
                        if (query.Execute(dst_name, out var res, timeout))
                        {
                        }
                        else
                        {
                            r.Error("Отметка об успешном получении сообщения в очереди ms", res.e);
                            return false;
                        }
                    }
                    else if (dst_type == "ms_direct")
                    {
                        string dst_complete_proc_do = dst_complete_proc?.Replace("{values}", ids.ConvertToDB());
                        string query = dst_complete_proc_do ?? "update t set " + dst_ready_flag_name + " = 1 from " + dst_table + " t inner join (" + ids.ConvertToDB() + ")s(" + src_id_name + ") on t." + src_id_name + " = s." + src_id_name + ";";
                        if (query.Execute(out var res, dst_name, timeout))
                        {
                        }
                        else
                        {
                            r.Error("Отметка об успешном получении сообщения в очереди ms_direct", res.e);
                            return false;
                        }

                    }
                    else if (dst_type == "pg")
                    {
                        string dst_complete_proc_do = dst_complete_proc?.Replace("{array}", ids.ConvertToDB());
                        string query = dst_complete_proc_do ?? "update " + dst_table + " set " + dst_ready_flag_name + " = true where " + src_id_name + " = any(" + ids.ConvertToDB() + ");";
                        if (query.ExecutePg(dst_name, out var res, timeout))
                        {
                        }
                        else
                        {
                            r.Error("Отметка об успешном получении сообщения в очереди pg", res.e);
                            return false;
                        }
                    }
                    else if (dst_type == "pg_direct")
                    {
                        string dst_complete_proc_do = dst_complete_proc?.Replace("{array}", ids.ConvertToDB());
                        string query = dst_complete_proc_do ?? "update " + dst_table + " set " + dst_ready_flag_name + " = true where " + src_id_name + " = any(" + ids.ConvertToDB() + ");";
                        if (query.ExecutePg(out var res, dst_name, timeout))
                        {
                        }
                        else
                        {
                            r.Error("Отметка об успешном получении сообщения в очереди pg_direct", res.e);
                            return false;
                        }
                    }
                }
                else
                {
                    need_complete = false;
                }
            }

            return true;
        }

        public static bool QueueLoad(Rule r, bool set_complete = true)
        {
            // Грузим то что накопилось в очереди и не обработано
            // Грузим сообщения
            // Грузим то что накопилось в очереди и не обработано
            // Делаем Жоб

            var res = QueueLoadComplete(r) && CopyToTable(r, false) && QueueLoadComplete(r);
            if (res)
            {
                if (r.Query.IsNullOrWhiteSpace())
                {
                    if (set_complete)
                        r.Complete();
                }
                else
                {
                    res = Job.DoJob(r, src_type : r.DstType,
                                        src_name: r.DstName,
                                        src_url: null,
                                        query: r.Query,
                                        limit: r.Limit,
                                        timeout: r.Timeout,
                                        set_complete: set_complete);
                }
            }
            return res;
        }


        public static bool CopyToService(Rule r, bool set_complete = true)
        {
            var src_type = r.SrcType;
            var src_name = r.SrcName;

            var dst_type = r.DstType;
            var dst_name = r.DstName;

            var dst_field_name = r.DstFieldName;

            var timeout = r.Timeout;
            var limit = r.Limit;

            var src_table = r.SrcTable;

            var src_complete_proc = r.SrcCompleteProc;

            var src_id_name = r.SrcIDName;

            var src_query = r.SrcQuery ?? "select * from " + src_table + (src_id_name.IsNullOrWhiteSpace() ? "" : " order by " + src_id_name) + ";";

            long? _id = null;

            var dst_url = r.DstUrl;

            Func<List<Tuple<Type, string>>, List<Dictionary<string, object>>, Tuple<bool, Exception>> write_func = null;

            if (dst_type == "mc")
            {
                write_func = (f, d) =>
                {
                    if (d.Any())
                    {
                        _id = d.Max(ff => ff.GetElement<long>(src_id_name));
                    }

                    var res = HttpQuery.CallService(dst_url, dst_name, timeoutSeconds: timeout, args: new DSO() { { dst_field_name, d } }).GetAwaiter().GetResult();

                    if (res.Item2 == System.Net.HttpStatusCode.OK)
                    {
                        var js = res.Item1.TryGetFromJson();
                        return new Tuple<bool, Exception>(true, null);
                    }
                    else
                    {
                        var ex = new Exception(res.Item1);
                        r.Error("Ошибка выполнения записи в mc", ex);
                        return new Tuple<bool, Exception>(false, ex);
                    }
                };
            }


            string src_complete_proc_do = src_complete_proc?.Replace("{id}", _id.ConvertToDB());
            string src_c_query = src_complete_proc_do ?? "delete from " + src_table + " where " + src_id_name +" <= " + _id.ConvertToDB() + ";";

            if (src_type == "ms")
            {
                if (src_query.Execute_Step(src_name, out var res, limit,
                        write_func, timeout))
                {
                    if (_id.HasValue)
                    {
                        if (!src_c_query.Execute(src_name, out var res_c, timeout))
                        {
                            r.Error("Запись факта прочтения с ошибкой", res_c.e);
                        }
                    }
                    
                    if (set_complete)
                        r.Complete();
                    return true;
                }
                else
                {
                    r.Error("Получение данных из источника ms", res.e);
                }
            }
            else if (src_type == "ms_direct")
            {
                if (src_query.Execute_Step(out var res, src_name, limit,
                        write_func, timeout))
                {
                    if (_id.HasValue)
                    {
                        if (!src_c_query.Execute(out var res_c, src_name, timeout))
                        {
                            r.Error("Запись факта прочтения с ошибкой", res_c.e);
                        }
                    }

                    if (set_complete)
                        r.Complete();
                    return true;
                }
                else
                {
                    r.Error("Получение данных из источника ms_direct", res.e);
                }
            }
            else if (src_type == "pg")
            {
                if (src_query.ExecutePg_Step(src_name, out var res, limit,
                        write_func, timeout))
                {
                    if (_id.HasValue)
                    {
                        if (!src_c_query.ExecutePg(src_name, out var res_c, timeout))
                        {
                            r.Error("Запись факта прочтения с ошибкой", res_c.e);
                        }
                    }

                    if (set_complete)
                        r.Complete();
                    return true;
                }
                else
                {
                    r.Error("Получение данных из источника pg", res.e);
                }
            }
            else if (src_type == "pg_direct")
            {
                if (src_query.ExecutePg_Step(out var res, src_name, limit,
                        write_func, timeout))
                {
                    if (_id.HasValue)
                    {
                        if (!src_c_query.ExecutePg(out var res_c, src_name, timeout))
                        {
                            r.Error("Запись факта прочтения с ошибкой", res_c.e);
                        }
                    }

                    if (set_complete)
                        r.Complete();
                    return true;
                }
                else
                {
                    r.Error("Получение данных из источника pg_direct", res.e);
                }
            }
            
            return false;
        }
    }
}
