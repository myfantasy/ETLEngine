using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace MyFantasy.ETLEngine
{
    public class Rule
    {
        public Dictionary<string, object> Params = new Dictionary<string, object>();

        public Dictionary<string, Action<Rule>> ExecuteFuncs = new Dictionary<string, Action<Rule>>()
        {
            { "Job",  Common.Job.DoJob },
            { "QueueLoad", (r) => Common.CopyTable.QueueLoad(r) },
            { "CopyToTable", (r) => Common.CopyTable.CopyToTable(r) },
            { "ReloadRules",  Dispatcher.ReloadRules }
        };        


        public DateTime? LastStart = null;
        public DateTime? LastFinish = null;

        public string RuleName { get { return Params.GetElement<string>("rule_name"); } }

        public string Type { get { return Params.GetElement<string>("type"); } }
        public string SrcType { get { return Params.GetElement<string>("src_type"); } }
        public string SrcName { get { return Params.GetElement<string>("src_name"); } }
        public string DstType { get { return Params.GetElement<string>("dst_type"); } }
        public string DstName { get { return Params.GetElement<string>("dst_name"); } }
        public string SrcUrl { get { return Params.GetElement<string>("src_url"); } }
        public string Query { get { return Params.GetElement<string>("query"); } }
        public string SrcTable { get { return Params.GetElement<string>("src_table"); } }
        public string SrcIDName { get { return Params.GetElement<string>("src_id_name"); } }
        public string DstTable { get { return Params.GetElement<string>("dst_table"); } }
        public int Timeout { get { return (int)Params.GetElement("timeout").TryParseOrDefault(10L); } }
        public int Limit { get { return (int)Params.GetElement("limit").TryParseOrDefault(1000L); } }
        public string DstReadyFlagName { get { return Params.GetElement<string>("dst_ready_flag_name"); } }
        public string SrcCompliteProc { get { return Params.GetElement<string>("src_complite_proc"); } }
        public string DstPrepareCompliteProc { get { return Params.GetElement<string>("dst_prepare_complite_proc"); } }
        public string DstCompliteProc { get { return Params.GetElement<string>("dst_complite_proc"); } }

        public long? RepeatTimeout { get { return Params.GetElement<long?>("repeat_timeout"); } }

        public bool isEnable = true;


        public string ErrorUrl { get { return Params.GetElement<string>("error_url"); } }
        public string CompliteUrl { get { return Params.GetElement<string>("complite_url"); } }

        public static string GlobalErrorUrl = null;
        public static string GlobalCompliteUrl = null;

        public static List<Rule> LoadSettingsFromFile(string file_name)
        {
            List<Rule> res = new List<Rule>();
            if (file_name.Like("http://") || file_name.Like("https://"))
            {
                var r = HttpQuery.CallServiceGet(file_name).GetAwaiter().GetResult();
                if (r.Item2 == System.Net.HttpStatusCode.OK)
                {
                    res.AddRange(LoadSettings(r.Item1));
                }
            }
            else if (!file_name.IsNullOrWhiteSpace() && File.Exists(file_name))
            {
                string s = File.ReadAllText(file_name);

                res.AddRange(LoadSettings(s));
            }
            else
            {
            }
            return res;
        }

        public static List<Rule> LoadSettingsFromDSO(Dictionary<string, object> dso)
        {
            var res = new List<Rule>();
            string type = dso.GetElement<string>("type");
            if (type.In("pg","ms", "ms_direct", "pg_direct"))
            {
                DataResult dr = null;
                string query = dso.GetElement<string>("query");
                string src_name = dso.GetElement<string>("src_name");

                if (type == "pg" && query.ExecutePg(src_name, out dr) || type == "pg_direct" && query.ExecutePg(out dr, src_name)
                    || type == "ms" && query.Execute(src_name, out dr) || type == "ms_direct" && query.Execute(out dr, src_name))
                {
                    if (dr.res.Any())
                    {
                        string s = dr.res[0][0].FirstOrDefault().Value.ToString();
                        res.AddRange(LoadSettings(s));
                    }
                }
                
            }
            return res;
        }

        public static List<Rule> LoadSettings(string json)
        {
            List<Rule> res = new List<Rule>();
            var js = json.TryGetFromJson();
            var conn_settings = js.GetElement<LO>("conn_settings");

            GlobalErrorUrl = js.ContainsElement("global_error_url") ? js.GetElement<string>("global_error_url") : GlobalErrorUrl;
            GlobalCompliteUrl = js.ContainsElement("global_error_url") ? js.GetElement<string>("global_complite_url") : GlobalCompliteUrl;

            if (!conn_settings.IsNullOrEmpty())
            {
                foreach (var cs in conn_settings)
                {
                    string type = cs.GetElement_DO<string>("type");
                    string conn_string = cs.GetElement_DO<string>("cs");
                    List<object> conn_string_lo = cs.GetElement_DO<List<object>>("cs");
                    string name = cs.GetElement_DO<string>("name");
                    if (!conn_string.IsNullOrWhiteSpace())
                    {
                        if (type == "ms")
                        {
                            QueryHandler.ConnStrings.AddOrUpdate(name, conn_string);
                        }
                        if (type == "pg")
                        {
                            QueryHandlerPg.ConnStrings.AddOrUpdate(name, conn_string);
                        }
                        if (type == "mc")
                        {
                            HttpQuery.McServers.AddOrUpdate(name, conn_string);
                        }
                    }

                    if (conn_string_lo != null)
                    {
                        var conn_group = QueryHandler.ConnStringGroup.Create(conn_string_lo);
                        if (type == "ms")
                        {
                            QueryHandler.ConnGroups.AddOrUpdate(name, conn_group);
                        }
                        if (type == "pg")
                        {
                            QueryHandlerPg.ConnGroups.AddOrUpdate(name, conn_group);
                        }
                        if (type == "mc")
                        {
                            HttpQuery.McGroups.AddOrUpdate(name, conn_group);
                        }
                    }
                }
            }

            var rules = js.GetElement<LO>("rules");
            if (!rules.IsNullOrEmpty())
            {
                foreach (var cs in rules)
                {
                    var d = cs as Dictionary<string, object>;
                    if (d != null)
                    {
                        Rule r = new Rule() { Params = d };
                        res.Add(r);
                    }
                }
            }
            var ext_settings = js.GetElement<LO>("ext_settings");
            if (!ext_settings.IsNullOrEmpty())
            {
                foreach (var cs in ext_settings)
                {
                    if (cs is string)
                    {
                        string file_name = cs as string;
                        res.AddRange(LoadSettingsFromFile(file_name));
                    }
                    else if (cs is Dictionary<string, object>)
                    {
                        Dictionary<string, object> dso = cs as Dictionary<string, object>;
                        res.AddRange(LoadSettingsFromDSO(dso));
                    }
                }
            }

            return res;
        }

        public Task t = null;

        public void Execute()
        {
            try
            {
                string type = Type;
                if (ExecuteFuncs.TryGetValue(type, out var a))
                {
                    a(this);
                }
            }
            catch (Exception ex)
            {
                Error(ex);
            }
        }

        public void ExecuteInTask()
        {
            if (t == null || t.Status == TaskStatus.RanToCompletion || t.Status == TaskStatus.Canceled || t.Status == TaskStatus.Faulted)
            {
                Action a = Execute;
                t = new Task(() =>
                {
                    lock (this)
                    {
                        try
                        {
                            LastStart = DateTime.Now;
                            a();
                            LastFinish = DateTime.Now;
                        }
                        catch (Exception ex)
                        {
                            Error(ex);
                            LastFinish = DateTime.Now;
                        }
                    }
                });
                // LOG There
                t.Start();
            }
        }

        public void Error(Exception ex)
        {
            OnError?.Invoke(this, ex);

            string url = ErrorUrl ?? GlobalErrorUrl;

            if (!url.IsNullOrWhiteSpace())
            {
                HttpQuery.CallService(url, new Dictionary<string, object>() { { "exception", ex.Message }, { "trace", ex.StackTrace }, { "name", RuleName }, { "date_last_start", LastStart }, { "date_err", DateTime.Now } }, timeoutSeconds: 10).GetAwaiter().GetResult();
            }
        }
        public void Complite()
        {
            OnComplite?.Invoke(this);

            string url = CompliteUrl ?? GlobalCompliteUrl;

            if (!url.IsNullOrWhiteSpace())
            {
                HttpQuery.CallService(url, new Dictionary<string, object>() { { "name", RuleName }, { "date_last_start", LastStart }, { "date_complite", DateTime.Now } }, timeoutSeconds: 10).GetAwaiter().GetResult();
            }
        }

        public static event Action<Rule> OnComplite;
        public static event Action<Rule, Exception> OnError;
    }
}
