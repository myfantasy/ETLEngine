using MyFantasy.ETLEngine;
using System;
using System.Threading.Tasks;

namespace ETLBot
{
    class Program
    {
        static void Main(string[] args)
        {
            System.Linq.Linq.OnGlobalException += Linq_OnGlobalException;

            Dispatcher.ReloadRules("rules.json");
            Task t = new Task(Dispatcher.RunAllRulesLoop);
            t.Start();
            Console.WriteLine("press any key to stop");
            Console.ReadKey();
            Dispatcher._Do = false;
            t.Wait();
            Console.WriteLine();
            Console.WriteLine("press any key to exit");
            Console.ReadKey();
        }

        private static void Linq_OnGlobalException(Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }
    }
}
