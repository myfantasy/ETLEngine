using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Http;
using System.Text;

namespace MyFantasy.ETLEngine.Common
{
    public static class Job
    {
        public static void DoRule(Rule r)
        {
            var src_type = r.SrcType;
            var src_name = r.SrcName;
            var src_url = r.SrcUrl;
            var query = r.Query;
            var limit = r.Limit;

            var timeout = r.Timeout;

            bool recall = true;
            int i = 0;

            while (recall && (i == 0 || i < limit))
            {
                recall = false;
                i++;


                if (src_type == "ms")
                {
                    if (query.Execute(src_name, out var result, timeout))
                    {
                        if (result.exists_row && result.res[0][0].Any(f => f.Value is bool && (bool)f.Value))
                        {
                            recall = true;
                        }
                    }
                    else
                    {
                        r.Error(result.e);
                        return;
                    }
                }
                else if (src_type == "ms_direct")
                {
                    if (query.Execute(out var result, src_name, timeout))
                    {
                        if (result.exists_row && result.res[0][0].Any(f => f.Value is bool && (bool)f.Value))
                        {
                            recall = true;
                        }
                    }
                    else
                    {
                        r.Error(result.e);
                        return;
                    }

                }
                else if (src_type == "pg")
                {
                    if (query.ExecutePg(src_name, out var result, timeout))
                    {
                        if (result.exists_row && result.res[0][0].Any(f => f.Value is bool && (bool)f.Value))
                        {
                            recall = true;
                        }
                    }
                    else
                    {
                        r.Error(result.e);
                        return;
                    }

                }
                else if (src_type == "pg_direct")
                {
                    if (query.ExecutePg(out var result, src_name, timeout))
                    {
                        if (result.exists_row && result.res[0][0].Any(f => f.Value is bool && (bool)f.Value))
                        {
                            recall = true;
                        }
                    }
                    else
                    {
                        r.Error(result.e);
                        return;
                    }

                }
                else if (src_type == "mc")
                {
                    var res = HttpQuery.CallNamedServiceGet(src_url, src_name, timeoutSeconds: timeout).GetAwaiter().GetResult();

                    if (res.Item2 == System.Net.HttpStatusCode.OK)
                    {
                        var js = res.Item1.TryGetFromJson();

                        if (js != null && js.Any(f => f.Value is bool && (bool)f.Value))
                        {
                            recall = true;
                        }
                    }
                    else
                    {
                        r.Error(new Exception(res.Item1));
                        return;
                    }

                }
                else if (src_type == "mc_direct")
                {
                    var res = HttpQuery.CallServiceGet(src_name, timeoutSeconds: timeout).GetAwaiter().GetResult();

                    if (res.Item2 == System.Net.HttpStatusCode.OK)
                    {
                        var js = res.Item1.TryGetFromJson();

                        if (js != null && js.Any(f => f.Value is bool && (bool)f.Value))
                        {
                            recall = true;
                        }
                    }
                    else
                    {
                        r.Error(new Exception(res.Item1));
                        return;
                    }
                }
            }

            r.Complite();
        }
    }
}
