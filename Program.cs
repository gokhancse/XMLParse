using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Configuration;
using System.Xml;
using System.Xml.Linq;
using System.IO;
using System.Data;
using System.Data.SqlClient;

namespace ParseXml
{
    class Program
    {
       public static string newxml = "";
       public static string constr = "Server=.;Database=Sample;Trusted_Connection=True;";
        public static void Parse()
        {
            //var fileList = Directory.GetFiles(@"C:\\Users\\gökhan\\Desktop\\deneme", "*", SearchOption.AllDirectories);
           DataTable dt = CreateDataTableXML(newxml);
           string Query = CreateTableQuery(dt);
           SqlConnection con = new SqlConnection(constr);
           con.Open();
           SqlCommand cmd = new SqlCommand("IF OBJECT_ID('dbo." +
           dt.TableName + "', 'U') IS NOT NULL DROP TABLE dbo." + dt.TableName + ";", con);
           cmd.ExecuteNonQuery();
         
           cmd = new SqlCommand(Query, con);
           int check = cmd.ExecuteNonQuery();
           if (check != 0)
           {
               using (var copy = new SqlBulkCopy
                     (con.ConnectionString, SqlBulkCopyOptions.KeepIdentity))
               {
                   // my DataTable column names match my SQL Column names,
                   // so I simply made this loop.
                   //However if your column names don't match,
                   //just pass in which datatable name matches the SQL column name in Column Mappings
                   foreach (DataColumn col in dt.Columns)
                   {
                   copy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                   }
                     copy.BulkCopyTimeout = 600;                     
                     copy.DestinationTableName = dt.TableName;       
                     copy.WriteToServer(dt);                         
               }
           }
           con.Close();
        }
        // XML için tablo oluştur
        public static DataTable CreateDataTableXML(string XmlFile)
        {
            XmlDocument xDoc = new XmlDocument();
           // string file = "MY-10--201906252130.xml";
            xDoc.Load(XmlFile);
            XmlNode node = xDoc.DocumentElement.ChildNodes.Cast<XmlNode>().ToList()[0];

            DataTable dt = new DataTable();
            dt.TableName = node.Name;
            foreach (XmlNode item in node.ChildNodes)
            {
                if (item.Name.ToString() == "tMaxDbirgun" || item.Name.ToString() == "istad") // Yeni XML'in hangi sütunları SQL'e yazılacaksa o ayarlanır, kodlar düzenlenir.
                {
                    dt.Columns.Add(item.Name,typeof(String));
                }
            }
            XmlElement root = xDoc.DocumentElement;
            string snode = "/"+root.Name+"/"+node.Name;
              
                XmlNodeList nodeList = xDoc.SelectNodes(snode);
                DataRow dr;
                foreach (XmlNode item in nodeList)
                {
                    dr = dt.NewRow();
                    dr["tMaxDbirgun"]= item.SelectSingleNode("tMaxDbirgun").InnerText.ToString();// İstenilen farklı şekillerde eşitlendirilebir..
                    dr["istad"] = item.SelectSingleNode("istad").InnerText.ToString();
                    dt.Rows.Add(dr);
                }

                //var value = xe.GetElementsByTagName("tMaxDbirgun");
                //foreach (XmlElement item in value)
                //{
                //    //List<string> Valores = Fila.ChildNodes.Cast<XmlNode>().
                //    //                 ToList().Select(x => x.InnerText).ToList();

                //    //Dt.Rows.Add(Valores.ToArray());

                //    //list.Add(item.InnerText);
                //    dt.Rows.Add(item.InnerText.ToString());
                //    //Progress();
                //}
            return dt;
        }

        public static string CreateTableQuery(DataTable table)
        {
            string tablequery = "CREATE TABLE " + table.TableName + "(";
            for (int i = 0; i < table.Columns.Count; i++)
            {
                tablequery += "[" + table.Columns[i].ColumnName + "]";
                string columnType = table.Columns[i].DataType.ToString();
                switch (columnType)
                {
                    case "System.Int32":
                        tablequery += " int ";
                        break;
                    case "System.Int64":
                        tablequery += " bigint ";
                        break;
                    case "System.Int16":
                        tablequery += " smallint";
                        break;
                    case "System.Byte":
                        tablequery += " tinyint";
                        break;
                    case "System.Decimal":
                        tablequery += " decimal ";
                        break;
                    case "System.DateTime":
                        tablequery += " datetime ";
                        break;
                    case "System.String":
                    default:
                        tablequery += string.Format(" nvarchar({0}) ",
                        table.Columns[i].MaxLength == -1 ? "max" :
                        table.Columns[i].MaxLength.ToString());
                        break;
                }
                if (table.Columns[i].AutoIncrement)
                    tablequery += " IDENTITY(" + table.Columns[i].AutoIncrementSeed.ToString() +
                    "," + table.Columns[i].AutoIncrementStep.ToString() + ") ";
                if (!table.Columns[i].AllowDBNull)
                    tablequery += " NOT NULL ";
                tablequery += ",";

            }
            string s= tablequery.Substring(0, tablequery.Length - 1)+")";
            return s;
        }
        static void Main(string[] args)
        {
            FileSystemWatcher watcher = new FileSystemWatcher();
            watcher.Path = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            watcher.EnableRaisingEvents = true;
            watcher.NotifyFilter = NotifyFilters.FileName;
            watcher.Filter = "*.xml";
            watcher.Created += new FileSystemEventHandler(OnChanged);
            // Uygulama açık kaldığı sürece bekliyor, yeni xml monitor olarak seçicen debug folder altına geldiği anda uygulama SQL'e parse ediyor.
            new System.Threading.AutoResetEvent(false).WaitOne();
        }

        private static void OnChanged(object sender, FileSystemEventArgs e)
        {
            newxml = e.Name;
            Parse();
            Console.WriteLine("Parsed Succesfully!");
        }

    }
}
