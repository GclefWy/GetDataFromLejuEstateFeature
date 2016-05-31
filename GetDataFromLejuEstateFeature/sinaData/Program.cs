using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace sinaData
{
    class Program
    {
        public static void WriteLog(string log)
        {

            //string str = System.Environment.CurrentDirectory;
            if (File.Exists(Environment.CurrentDirectory + @"/log/log-" + DateTime.Today.ToShortDateString().Replace("/", "-") + ".txt"))
            {
                string strFilePath = Environment.CurrentDirectory + @"/log/log-" + DateTime.Today.ToShortDateString().Replace("/", "-") + ".txt";
                FileStream fs = new FileStream(strFilePath, FileMode.Append);
                StreamWriter sw = new StreamWriter(fs, Encoding.Default);
                sw.WriteLine(DateTime.Now.ToString() + "|" + log);
                sw.Close();
                fs.Close();
            }
            else
            {
                string strFilePath = Environment.CurrentDirectory + @"/log/log-" + DateTime.Today.ToShortDateString().Replace("/", "-") + ".txt";
                File.Create(strFilePath).Dispose();
                FileStream fs = new FileStream(strFilePath, FileMode.Append);
                StreamWriter sw = new StreamWriter(fs, Encoding.Default);
                sw.WriteLine(DateTime.Now.ToString() + "|" + log);
                sw.Close();
                fs.Close();
            }
        }

        static void Main(string[] args)
        {

            try
            {
                if (args.Length < 1)
                {
                    Console.WriteLine(@"请输入城市代码 [cityname]");
                    return;
                }

                string cityCode = args[0];
                string sql = @"select max(update_timestamp) from TB_ESTATE_HID_LIST where city='" + cityCode + "'";
                DataSet ds = SimpleDataHelper.Query(SimpleDataHelper.MSConnectionString, sql);
                int startTimeStamp = 0;

                if (ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0 && ds.Tables[0].Rows[0][0].ToString().Length > 0)
                {
                    startTimeStamp = Int32.Parse(ds.Tables[0].Rows[0][0].ToString());
                }

                Dictionary<string, string> param = new Dictionary<string, string>();
                //param.Add("timestamp", "1463130801");
                //param.Add("noncestr", "ibReDxJYb4QoMKTC");
                //param.Add("city", "bj");
                int timeStamp = SinaSignHelper.ConvertDateTimeInt(DateTime.Now);
                param.Add("timestamp", timeStamp.ToString());
                param.Add("noncestr", SinaSignHelper.generateNonceStr(16));
                param.Add("city", cityCode);
                param.Add("signature", SinaSignHelper.getSignature(param, "6ba895293c27f610").ToLower());
                param.Add("key", "6ba895293c27f610");
                param.Add("return", "json");
                param.Add("encode", "utf-8");
                //param.Add("module", "houseinfo");

                string URL = @"http://data.house.sina.com.cn/api/api.agent.php";
                string resultObjName;

                if (startTimeStamp == 0)
                {
                    param.Add("module", "get_hids");
                    resultObjName = "get_hids";
                }
                else
                {
                    param.Add("module", "update_hids");
                    resultObjName = "update_hids";
                    param.Add("start", startTimeStamp.ToString());
                }

                int tryCount = 10;

                while (tryCount>0)
                {
                    string rtn = HttpHelper.getHttp(URL, param);

                    Console.WriteLine(rtn);
                    WriteLog("getData : " + rtn);

                    if (rtn.Length > 0)
                    {

                        try
                        {
                            JObject jo = JObject.Parse(rtn);

                            for (int i = 0; i < jo[resultObjName].Count(); i++)
                            {
                                string d_hid = (string)jo[resultObjName][i];

                                sql = "delete from TB_ESTATE_HID_LIST where city = '" + cityCode + "' and hid=" + d_hid + "; ";
                                sql += "insert into TB_ESTATE_HID_LIST ([hid],[city],[update_timestamp]) ";
                                sql += "values (" + d_hid + ",'" + cityCode + "'," + timeStamp.ToString() + "); ";

                                Console.WriteLine(sql);

                                SimpleDataHelper.Excsql(SimpleDataHelper.MSConnectionString, sql);
                            }

                            break;
                        }
                        catch (Exception ex)
                        {
                            WriteLog(ex.Message);
                        }
                    }

                    tryCount--;
                }






                string sql2 = @"select max(update_timestamp) from LEJU_ESTATE_BASEINFO where city='" + cityCode + "'";
                DataSet ds2 = SimpleDataHelper.Query(SimpleDataHelper.MSConnectionString, sql2);
                int lastTimeStamp = 0;
                if (ds2.Tables.Count > 0 && ds2.Tables[0].Rows.Count > 0 && ds2.Tables[0].Rows[0][0].ToString().Length > 0)
                {
                    lastTimeStamp = Int32.Parse(ds2.Tables[0].Rows[0][0].ToString());
                }

                sql2 = @"select hid, update_timestamp from TB_ESTATE_HID_LIST where city='" + cityCode + "'";
                if (lastTimeStamp > 0)
                {
                    sql2 += @" and update_timestamp>" + lastTimeStamp;
                }
                ds2 = SimpleDataHelper.Query(SimpleDataHelper.MSConnectionString, sql2);

                if (ds2.Tables.Count > 0 && ds2.Tables[0].Rows.Count > 0)
                {
                    int tryCount = 0;
                    int i = 0;
                    while (i < ds2.Tables[0].Rows.Count)
                    //for (int i = 0; i < 1; i++)
                    {
                        Console.WriteLine(i.ToString() + " , " + ds2.Tables[0].Rows.Count.ToString());
                        string m_hid = ds2.Tables[0].Rows[i][0].ToString();
                        string m_update_timestamp = ds2.Tables[0].Rows[i][1].ToString();

                        Dictionary<string, string> param2 = new Dictionary<string, string>();
                        //param.Add("timestamp", "1463455362");
                        //param.Add("noncestr", "fpPdxWUIHr7tRenD");
                        //param.Add("city", "bj");
                        int timeStamp2 = SinaSignHelper.ConvertDateTimeInt(DateTime.Now);
                        param2.Add("timestamp", timeStamp2.ToString());
                        param2.Add("noncestr", SinaSignHelper.generateNonceStr(16));
                        param2.Add("city", cityCode);
                        param2.Add("hid", m_hid);
                        //param2.Add("timestamp", "1463130801");
                        //param2.Add("noncestr", "ibReDxJYb4QoMKTC");
                        //param2.Add("city", "bj");
                        //param2.Add("hid", "129423");
                        param2.Add("signature", SinaSignHelper.getSignature(param2, "6ba895293c27f610").ToLower());
                        param2.Add("key", "6ba895293c27f610");
                        param2.Add("module", "houseinfo");
                        param2.Add("return", "json");
                        param2.Add("encode", "utf-8");

                        string rtn2 = HttpHelper.getHttp(URL, param2);

                        Console.WriteLine(m_hid);
                        //Console.WriteLine(rtn2);

                        if (rtn2.Length > 0)
                        {
                            JObject jo = JObject.Parse(rtn2);
                            JObject houseinfo = (JObject)jo["houseinfo"];
                            JObject baseinfo;
                            try
                            {
                                baseinfo = (JObject)houseinfo["baseinfo"][0];
                            }
                            catch
                            {
                                baseinfo = null;
                            }


                            if (baseinfo != null)
                            {
                                sql2 = "delete from LEJU_ESTATE_BASEINFO where city = '" + cityCode + "' and hid=" + m_hid + "; ";
                                //Console.WriteLine(sql2);

                                SimpleDataHelper.Excsql(SimpleDataHelper.MSConnectionString, sql2);

                                DataTable dt = new DataTable();
                                dt.Columns.Add("hid", typeof(Int32));
                                dt.Columns.Add("city", typeof(string));
                                dt.Columns.Add("update_timestamp", typeof(Int32));
                                dt.Columns.Add("result_json", typeof(string));
                                dt.Columns.Add("is_new", typeof(Int32));

                                //DataSet ds3 = SimpleDataHelper.Query(SimpleDataHelper.MSConnectionString, sql);
                                System.Data.DataRow dataRow;
                                dataRow = dt.NewRow();
                                dataRow["hid"] = m_hid;
                                dataRow["city"] = cityCode;
                                dataRow["update_timestamp"] = m_update_timestamp;
                                dataRow["result_json"] = rtn2;
                                dataRow["is_new"] = 1;

                                dt.Rows.Add(dataRow);

                                SimpleDataHelper.SqlBCP(SimpleDataHelper.MSConnectionString, dt, "LEJU_ESTATE_BASEINFO");
                                tryCount = 0;
                            }
                            else
                            {
                                tryCount++;
                                if (tryCount > 10)
                                {
                                    tryCount = 0;
                                    WriteLog(m_hid + " : " + rtn2);
                                    Console.WriteLine(m_hid + " : " + rtn2);

                                }
                                else
                                {
                                    i--;
                                    WriteLog("Retry : " + m_hid + " : " + rtn2);
                                    Console.WriteLine("Retry : " + m_hid + " : " + rtn2);
                                }
                            }

                        }

                        i++;

                    }
                }

            }
            catch (Exception ex)
            {
                WriteLog(ex.Message);

            }
        }
    }

}
