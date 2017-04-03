using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dbfMerger
{
    class Program
    {
        static void Main(string[] args)    
        {
            int ImportedCount = 0;
            int MergedCount = 0;
            int ExportedCount = 0;
            //šablóna databázy
            String sourceDBpath = @"C:\dbfMerger\dat\source.dbf";
            //adresa exortnej databazy
            String exportDBpath = @"C:\PRENOSY\BONAVITA\VSTUP\Bonavita.dbf";
            //cesta kde sa nachadzaju subory ktore treba spojit
            string path = @"C:\D-flex-import\";


            Console.WriteLine("Started");
            //naskenovanie vsetkych suborov s priponou .dbf
            string[] files = System.IO.Directory.GetFiles(path,"*.dbf");
            Console.WriteLine("Počet databázových súborov v priečinku "+ path + ": "+ files.Count());


            string connectionString = "Provider=Microsoft.Jet.OLEDB.4.0; Data Source="+System.IO.Path.GetDirectoryName(files[0]) + ";Extended Properties=DBASE III;";
            OleDbConnection conn = new OleDbConnection(connectionString);
            
            conn.Open();

            List<DataTable> importTables = new List<DataTable>();
            //v cykle sa vyselektuju vsetky zaznamy z kazdeho suboru a ulozia sa ako DataTable do kolekcie importTables
            foreach (string file in files)
            {
                Console.WriteLine("Čítam databázu: " + file);
                string strQuery = "SELECT * FROM [" + System.IO.Path.GetFileName(file) + "]";
                OleDbDataAdapter adapter = new OleDbDataAdapter(strQuery, conn);

                DataTable dt = new DataTable();
                adapter.Fill(dt);
                importTables.Add(dt);
                ImportedCount += dt.Rows.Count;
                
            }
            conn.Close();
            Console.WriteLine("Hotovo.");
            Console.WriteLine("Počet  načítaných súborov: "+importTables.Count);

            //V cykle sa spoja vsetky naimportovane tabulky do jednej - exportTable
            DataTable exportTable = new DataTable();
            if(importTables.Count==1)
            {
                exportTable = importTables[0];
            }
            else if(importTables.Count>1)
            {
                //exportTable = importTables[0];
                foreach(DataTable t in importTables)
                {
                    exportTable.Merge(t);
                }
            }
            MergedCount += exportTable.Rows.Count;


            //vytvorenie prazdnej databazy s headermi resp. nakopirovanie so sablony

            

            if (System.IO.File.Exists(sourceDBpath))
            {
                System.IO.File.Copy(sourceDBpath, exportDBpath, true);
            }




            string ExconnectionString = "Provider=Microsoft.Jet.OLEDB.4.0; Data Source=" + System.IO.Path.GetDirectoryName(exportDBpath) + ";Extended Properties=DBASE III;";
            try
            {
                OleDbConnection Exconn = new OleDbConnection(ExconnectionString);
                Exconn.Open();

                //kazdy jeden riadok z pripravenej spojenej databazy sa INSERTne do novej DB    
                Console.WriteLine("Zapisujem data");
                foreach (DataRow row in exportTable.Rows)
                {
                    OleDbCommand cmd = new OleDbCommand();
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "INSERT INTO " + System.IO.Path.GetFileName(exportDBpath) + " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

                    foreach (DataColumn column in exportTable.Columns)
                    {
                        cmd.Parameters.AddWithValue(column.ColumnName, row[column]);
                    }
                    cmd.Connection = Exconn;
                    cmd.ExecuteNonQuery();
                    Console.Write(".");
                    ExportedCount++;

                }
                Console.WriteLine();
                Console.WriteLine("Exportovane");
                Exconn.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                throw;
            }


            Console.WriteLine("IMPORTED: " + ImportedCount);
            Console.WriteLine("MERGED:   " + MergedCount);
            Console.WriteLine("INSERTED: " + ExportedCount);

            //premazanie adresara
            System.IO.Directory.Delete(path, true);
            System.IO.Directory.CreateDirectory(path);

            Console.WriteLine();
            Console.WriteLine("STLAČ ĽUB. KLÁVESU");
            Console.Read();
        }
    }
}
