using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace MyFantasy.ETLEngine
{
    public static class Dispatcher
    {
        public static Dictionary<string, Rule> Rules = new Dictionary<string, Rule>();

        public static void ReloadRules(Rule r)
        {
            ReloadRules(r.Query);
        }

        public static void ReloadRules(string path)
        {
            lock (Rules)
            {
                var rules = Rule.LoadSettingsFromFile(path);

                if (!(rules?.Any() ?? false))
                {
                    return;
                }

                Dictionary<string, bool> ar = new Dictionary<string, bool>();
                Dictionary<string, bool> nr = new Dictionary<string, bool>();

                foreach (var rl in rules)
                {
                    if (rl.RuleName != null)
                    {
                        if (Rules.ContainsKey(rl.RuleName))
                        {
                            Rules[rl.RuleName].Params = rl.Params;
                            Rules[rl.RuleName].isEnable = true;
                        }
                        else
                        {
                            Rules.Add(rl.RuleName, rl);
                        }
                        ar.Add(rl.RuleName, true);
                    }
                }

                foreach (var v in Rules)
                {
                    if (!ar.ContainsKey(v.Key))
                    {
                        nr.Add(v.Key, true);
                    }
                }

                foreach (var v in nr)
                {
                    Rules.Remove(v.Key);
                }
            }
        }

        public static bool _Do = true;

        public static TimeSpan Wait = new TimeSpan(0, 0, 1);

        public static void RunAllRulesLoop()
        {
            while (_Do)
            {
                try
                {
                    RunAllRules();
                    Thread.Sleep(Wait);
                }
                catch (Exception ex)
                {
                    Linq.OnErrorExecute(ex);
                }
            }
        }

        public static void RunAllRules()
        {
            lock (Rules)
            {
                foreach (var r in Rules)
                {
                    if (r.Value.RepeatTimeout.HasValue)
                    {
                        if (!r.Value.LastStart.HasValue || r.Value.LastFinish.HasValue && ((DateTime.Now - r.Value.LastFinish.Value).TotalMilliseconds >= r.Value.RepeatTimeout))
                        {
                            r.Value.ExecuteInTask();
                        }
                    }
                }
            }
        }
        
    }
}
