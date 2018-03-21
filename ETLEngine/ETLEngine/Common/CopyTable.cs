using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace MyFantasy.ETLEngine.Common
{
    public static class CopyTable
    {
        public static bool CopyToTable(Rule r, bool set_complite = true)
        {
            var src_type = r.SrcType;
            var src_name = r.SrcName;

            var dst_type = r.DstType;
            var dst_name = r.DstName;

            var src_id_name = r.SrcIDName;

            var src_query = r.Query ?? "select * from " + r.SrcTable + (src_id_name.IsNullOrWhiteSpace() ? "" : " order by " + src_id_name) + ";";
            var dst_table = r.DstTable;

            var timeout = r.Timeout;
            var limit = r.Limit;


            //string src_conn_string, string dst_conn_string, string src_query, string dst_table, int limit, int timeout, out Exception ex


            Func<List<Tuple<Type, string>>, List<Dictionary<string, object>>, bool> write_func = null;

            if (dst_type == "ms")
            {
                write_func = (f, d) =>
                {
                    if (dst_table.Execute_Bulk(dst_name, out var res_in, f, d, timeout))
                    {
                        return true;
                    }
                    else
                    {
                        r.Error(res_in.e);
                        return false;
                    }
                };
            }
            else if (dst_type == "ms_direct")
            {
                write_func = (f, d) =>
                {
                    if (dst_table.Execute_Bulk(out var res_in, dst_name, f, d, timeout))
                    {
                        return true;
                    }
                    else
                    {
                        r.Error(res_in.e);
                        return false;
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
                        return true;
                    }
                    else
                    {
                        r.Error(e);
                        return false;
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
                        return true;
                    }
                    else
                    {
                        r.Error(e);
                        return false;
                    }
                };
            }


            if (src_type == "ms")
            {
                if (src_query.Execute_Step(src_name, out var res, limit,
                        write_func, timeout))
                {
                    if (set_complite)
                        r.Complite();
                    return true;
                }
                else
                {
                    r.Error(res.e);
                }
            }
            else if (src_type == "ms_direct")
            {
                if (src_query.Execute_Step(out var res, src_name, limit,
                        write_func, timeout))
                {
                    if (set_complite)
                        r.Complite();
                    return true;
                }
                else
                {
                    r.Error(res.e);
                }
            }
            else if (src_type == "pg")
            {
                if (src_query.ExecutePg_Step(src_name, out var res, limit,
                        write_func, timeout))
                {
                    if (set_complite)
                        r.Complite();
                    return true;
                }
                else
                {
                    r.Error(res.e);
                }
            }
            else if (src_type == "pg_direct")
            {
                if (src_query.ExecutePg_Step(out var res, src_name, limit,
                        write_func, timeout))
                {
                    if (set_complite)
                        r.Complite();
                    return true;
                }
                else
                {
                    r.Error(res.e);
                }
            }
            
            return false;
        }


        public static bool QueueLoadComplite(Rule r)
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

            var dst_prepare_complite_proc = r.DstPrepareCompliteProc;
            var src_complite_proc = r.SrcCompliteProc;
            var dst_complite_proc = r.DstCompliteProc;

            List<object> ids = new List<object>();

            bool need_complite = true;

            while (need_complite)
            {
                if (dst_type == "ms")
                {
                    string query = dst_prepare_complite_proc ?? "select top(" + limit.ConvertToDB() + ") " + src_id_name + " from " + dst_table + " where " + dst_ready_flag_name + " = 0 order by " + src_id_name + ";";
                    if (query.Execute(dst_name, out var res, timeout))
                    {
                        ids = res.res[0].Select(f => f.GetElement(src_id_name)).ToList();
                    }
                    else
                    {
                        r.Error(res.e);
                        return false;
                    }
                }
                else if (dst_type == "ms_direct")
                {
                    string query = dst_prepare_complite_proc ?? "select top(" + limit.ConvertToDB() + ") " + src_id_name + " from " + dst_table + " where " + dst_ready_flag_name + " = 0 order by " + src_id_name + ";";
                    if (query.Execute(out var res, dst_name, timeout))
                    {
                        ids = res.res[0].Select(f => f.GetElement(src_id_name)).ToList();
                    }
                    else
                    {
                        r.Error(res.e);
                        return false;
                    }

                }
                else if (dst_type == "pg")
                {
                    string query = dst_prepare_complite_proc ?? "select " + src_id_name + " from " + dst_table + " where " + dst_ready_flag_name + " = false order by " + src_id_name + " limit " + limit.ConvertToDB() + ";";
                    if (query.ExecutePg(dst_name, out var res, timeout))
                    {
                        ids = res.res[0].Select(f => f.GetElement(src_id_name)).ToList();
                    }
                    else
                    {
                        r.Error(res.e);
                        return false;
                    }
                }
                else if (dst_type == "pg_direct")
                {
                    string query = dst_prepare_complite_proc ?? "select " + src_id_name + " from " + dst_table + " where " + dst_ready_flag_name + " = false order by " + src_id_name + " limit " + limit.ConvertToDB() + ";";
                    if (query.ExecutePg(out var res, dst_name, timeout))
                    {
                        ids = res.res[0].Select(f => f.GetElement(src_id_name)).ToList();
                    }
                    else
                    {
                        r.Error(res.e);
                        return false;
                    }
                }


                if (ids.Any())
                {

                    if (src_type == "ms")
                    {
                        string src_complite_proc_do = src_complite_proc?.Replace("{values}", ids.ConvertToDB());
                        string query = src_complite_proc_do ?? "delete t from " + src_table + " t inner join ("+ ids.ConvertToDB() + ")s("+ src_id_name + ") on t."+ src_id_name + " = s." + src_id_name + ";";
                        if (query.Execute(src_name, out var res, timeout))
                        {
                        }
                        else
                        {
                            r.Error(res.e);
                            return false;
                        }
                    }
                    else if (src_type == "ms_direct")
                    {
                        string src_complite_proc_do = src_complite_proc?.Replace("{values}", ids.ConvertToDB());
                        string query = src_complite_proc_do ?? "delete t from " + src_table + " t inner join (" + ids.ConvertToDB() + ")s(" + src_id_name + ") on t." + src_id_name + " = s." + src_id_name + ";";
                        if (query.Execute(out var res, src_name, timeout))
                        {
                        }
                        else
                        {
                            r.Error(res.e);
                            return false;
                        }
                    }
                    else if (src_type == "pg")
                    {
                        string src_complite_proc_do = src_complite_proc?.Replace("{array}", ids.ConvertToDB(FieldHandler.DbTypes.PG));
                        string query = src_complite_proc_do ?? "delete from " + src_table + " where " + src_id_name +" = any("+ ids.ConvertToDB() + ");";
                        if (query.ExecutePg(src_name, out var res, timeout))
                        {
                        }
                        else
                        {
                            r.Error(res.e);
                            return false;
                        }
                    }
                    else if (src_type == "pg_direct")
                    {
                        string src_complite_proc_do = src_complite_proc?.Replace("{array}", ids.ConvertToDB(FieldHandler.DbTypes.PG));
                        string query = src_complite_proc_do ?? "delete from " + src_table + " where " + src_id_name + " = any(" + ids.ConvertToDB() + ");";
                        if (query.ExecutePg(out var res, src_name, timeout))
                        {
                        }
                        else
                        {
                            r.Error(res.e);
                            return false;
                        }
                    }




                    if (dst_type == "ms")
                    {
                        string dst_complite_proc_do = dst_complite_proc?.Replace("{values}", ids.ConvertToDB());
                        string query = dst_complite_proc_do ?? "update t set "+ dst_ready_flag_name + " = 1 from " + dst_table + " t inner join (" + ids.ConvertToDB() + ")s(" + src_id_name + ") on t." + src_id_name + " = s." + src_id_name + ";";
                        if (query.Execute(dst_name, out var res, timeout))
                        {
                        }
                        else
                        {
                            r.Error(res.e);
                            return false;
                        }
                    }
                    else if (dst_type == "ms_direct")
                    {
                        string dst_complite_proc_do = dst_complite_proc?.Replace("{values}", ids.ConvertToDB());
                        string query = dst_complite_proc_do ?? "update t set " + dst_ready_flag_name + " = 1 from " + dst_table + " t inner join (" + ids.ConvertToDB() + ")s(" + src_id_name + ") on t." + src_id_name + " = s." + src_id_name + ";";
                        if (query.Execute(out var res, dst_name, timeout))
                        {
                        }
                        else
                        {
                            r.Error(res.e);
                            return false;
                        }

                    }
                    else if (dst_type == "pg")
                    {
                        string dst_complite_proc_do = dst_complite_proc?.Replace("{array}", ids.ConvertToDB());
                        string query = dst_complite_proc_do ?? "update " + dst_table + " set " + dst_ready_flag_name + " = true where " + src_id_name + " = any(" + ids.ConvertToDB() + ");";
                        if (query.ExecutePg(dst_name, out var res, timeout))
                        {
                        }
                        else
                        {
                            r.Error(res.e);
                            return false;
                        }
                    }
                    else if (dst_type == "pg_direct")
                    {
                        string dst_complite_proc_do = dst_complite_proc?.Replace("{array}", ids.ConvertToDB());
                        string query = dst_complite_proc_do ?? "update " + dst_table + " set " + dst_ready_flag_name + " = true where " + src_id_name + " = any(" + ids.ConvertToDB() + ");";
                        if (query.ExecutePg(out var res, dst_name, timeout))
                        {
                        }
                        else
                        {
                            r.Error(res.e);
                            return false;
                        }
                    }
                }
                else
                {
                    need_complite = false;
                }
            }

            return true;
        }

        public static bool QueueLoad(Rule r)
        {
            // Грузим то что накопилось в очереди и не обработано
            // Грузим сообщения
            // Грузим то что накопилось в очереди и не обработано

            var res = QueueLoadComplite(r) && CopyToTable(r, false) && QueueLoadComplite(r);
            if (res)
                r.Complite();
            return res;
        }

    }
}
