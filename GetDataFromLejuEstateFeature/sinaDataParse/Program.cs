using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sinaDataParse
{
    class Program
    {
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
                string sql = @"select * from LEJU_ESTATE_BASEINFO where is_new=1 and city='" + cityCode + "'";

                DataSet ds = SimpleDataHelper.Query(SimpleDataHelper.MSConnectionString, sql);
                if (ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
                {
                    for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
                    {
                        try
                        {
                            Int32 hid = (Int32)ds.Tables[0].Rows[i]["hid"];
                            string json = (string)ds.Tables[0].Rows[i]["result_json"];

                            string sql2 = @"delete from TB_ESTATE_FEATURES_BASEINFO where hid=" + hid.ToString() + " and City='" + cityCode + "';";
                            sql2 += @"delete from TB_ESTATE_FEATURES_PRICELIST where hid=" + hid.ToString() + " and City='" + cityCode + "';";
                            sql2 += @"delete from TB_ESTATE_FEATURES_PRICETREND where hid=" + hid.ToString() + " and City='" + cityCode + "';";
                            Console.WriteLine(sql2);

                            SimpleDataHelper.Excsql(SimpleDataHelper.MSConnectionString, sql2);

                            JObject jo = JObject.Parse(json);
                            JObject houseinfo = (JObject)jo["houseinfo"];
                            JObject baseinfo = (JObject)houseinfo["baseinfo"][0];
                            JObject house_price_trend = (JObject)houseinfo["house_price_trend"];
                            JObject district_price_trend = (JObject)houseinfo["district_price_trend"];

                            JArray d_price_trend_analysis = new JArray();
                            if (house_price_trend["price_trend_analysis"] is JObject)
                            {
                                foreach (JProperty jp in house_price_trend["price_trend_analysis"])
                                {
                                    d_price_trend_analysis.Add(jp.Value);
                                }

                            }
                            else
                            {
                                d_price_trend_analysis = (JArray)house_price_trend["price_trend_analysis"];
                            }

                            JArray d_tag = new JArray();
                            if (baseinfo["tags"] is JObject)
                            {
                                foreach (JProperty jp in (baseinfo["tags"]))
                                {
                                    d_tag.Add(jp.Value);
                                }

                            }
                            else
                            {
                                d_tag = (JArray)baseinfo["tags"];
                            }

                            string d_EstateName = (string)baseinfo["name"];
                            string d_EstateAddress = (string)baseinfo["address"];
                            string d_EstatePic = (string)baseinfo["pic_s320"];

                            string d_sale_schedule = (string)baseinfo["sale_schedule"];
                            string[] sArray = d_sale_schedule.Split(',');
                            int max_sale_schedule = 0;
                            foreach (string s in sArray)
                            {
                                if (int.Parse(s) > max_sale_schedule)
                                {
                                    max_sale_schedule = int.Parse(s);
                                }
                            }
                            string[] sale_schedule_Array = {"地块","开工","售楼处开放","排号","选房","开盘","顺销","售罄","交房" };
                            string d_SaleStateTag = "";
                            if ((0<max_sale_schedule) && (max_sale_schedule<10))
                            {
                                d_SaleStateTag = sale_schedule_Array[max_sale_schedule-1];
                            }

                            string d_EstateTag = "";

                            foreach (JObject jtag in d_tag)
                            {
                                 d_EstateTag += (string)jtag["title"]+",";
                            }

                            d_EstateTag = d_EstateTag.Substring(0, d_EstateTag.Length - 1);

                            string d_RoomType = (string)baseinfo["main_housetype"];
                            string d_OpenTime = (string)baseinfo["opentime"];
                            string d_LastPriceLabel = "";
                            string d_LastPriceValue = (string)house_price_trend["price_display"];
                            if (d_price_trend_analysis.Count > 0)
                            {
                                d_LastPriceValue += "|" + d_price_trend_analysis[0]["trend_descrip"];
                            }
                            string d_TrendPriceLabel = "";
                            string d_TrendPriceValue = "";
                            foreach (JObject item in d_price_trend_analysis)
                            {
                                d_TrendPriceValue += item["time"] + "|" + item["trend_descrip"] + "|";
                            }
                            if (d_TrendPriceValue.Length > 0)
                            {
                                d_TrendPriceValue = d_TrendPriceValue.Substring(0, d_TrendPriceValue.Length - 1);
                            }
                            string d_RegionLastPriceLabel = "";
                            string d_RegionLastPriceValue = (string)district_price_trend["price_avg_now"] + "|" + (string)district_price_trend["trend_descrip"] + "|" + int.Parse("0"+(string)district_price_trend["total"]).ToString() + "|" + int.Parse("0" + (string)district_price_trend["update_total"]).ToString() + "|" + int.Parse("0" + (string)district_price_trend["down_total"]).ToString() + "|" + int.Parse("0" + (string)district_price_trend["flat_total"]).ToString();


                            string d_HighPriceEstateCount = "0";
                            string d_LowPriceEstateCount = "0";
                            string d_FlatPriceEstateCount = "0";

                            //d_HighPriceEstateCount
                            try
                            {
                                d_HighPriceEstateCount = (string)district_price_trend["pk_list"]["up_total"];
                            }
                            catch
                            {
                                d_HighPriceEstateCount = "0";
                            }

                            try
                            {
                                d_LowPriceEstateCount = (string)district_price_trend["pk_list"]["down_total"];
                            }
                            catch
                            {
                                d_LowPriceEstateCount = "0";
                            }

                            try
                            {
                                d_FlatPriceEstateCount = (string)district_price_trend["pk_list"]["flat_total"];
                            }
                            catch
                            {
                                d_FlatPriceEstateCount = "0";
                            }

                            //如果highcount 〉0 则做list

                            string d_HighPriceEstate = "";
                            if (Convert.ToInt32(d_HighPriceEstateCount) > 0)
                            {
                                try
                                {
                                    foreach (JObject item in district_price_trend["pk_list"]["up_list"])
                                    {
                                        d_HighPriceEstate += item["hname"] + "|" + item["price"] + "|";
                                    }
                                    d_HighPriceEstate = d_HighPriceEstate.Substring(0, d_HighPriceEstate.Length - 1);
                                }
                                catch
                                {

                                }

                            }

                            //如果lowcount 〉0 则做list
                            string d_LowPriceEstate = "";
                            if (Convert.ToInt32(d_LowPriceEstateCount) > 0)
                            {
                                try
                                {
                                    foreach (JObject item in district_price_trend["pk_list"]["down_list"])
                                    {
                                        d_LowPriceEstate += item["hname"] + "|" + item["price"] + "|";
                                    }
                                    d_LowPriceEstate = d_LowPriceEstate.Substring(0, d_LowPriceEstate.Length - 1);
                                }
                                catch
                                {

                                }
                            }


                            //如果flatcount >0 则做list
                            string d_FlatPriceEstate = "";
                            if (Convert.ToInt32(d_FlatPriceEstateCount) > 0)
                            {
                                try
                                {
                                    foreach (JObject item in district_price_trend["pk_list"]["flat_list"])
                                    {
                                        d_FlatPriceEstate += item["hname"] + "|" + item["price"] + "|";
                                    }
                                    d_FlatPriceEstate = d_FlatPriceEstate.Substring(0, d_FlatPriceEstate.Length - 1);
                                }
                                catch
                                {

                                }

                            }

                            string d_PriceAnalysisLabel = "";
                            string d_PriceAnalysisValue = "";
                            try
                            {
                                d_PriceAnalysisValue = (string)district_price_trend["pk_list"]["total_new"] + "|" 
                                    + (string)district_price_trend["pk_list"]["up_total"] + "|"
                                    + (string)district_price_trend["pk_list"]["up_rate"] + "|"
                                     + (string)district_price_trend["pk_list"]["down_total"] + "|"
                                    + (string)district_price_trend["pk_list"]["down_rate"];
                            }
                            catch
                            {

                            }
                            string d_CreateTime = DateTime.Now.ToString();
                            string d_UpdateTime = d_CreateTime;
                            string d_State = "1";
                            string d_City = cityCode;
                            string d_is_new = "1";

                            string d_Price = "";
                            string d_PriceDate = "";



                            sql2 = @"Insert into TB_ESTATE_FEATURES_BASEINFO ("
                                + "hid"
                                + ",EstateName"
                                + ",EstateAddress"
                                + ",EstatePic"
                                + ",SaleStateTag"
                                + ",EstateTag"
                                + ",RoomType"
                                + ",OpenTime"
                                + ",LastPriceLabel"
                                + ",LastPriceValue"
                                + ",TrendPriceLabel"
                                + ",TrendPriceValue"
                                + ",RegionLastPriceLabel"
                                + ",RegionLastPriceValue"
                                + ",HighPriceEstateCount"
                                + ",LowPriceEstateCount"
                                + ",FlatPriceEstateCount"
                                + ",HighPriceEstate"
                                + ",LowPriceEstate"
                                + ",FlatPriceEstate"
                                + ",PriceAnalysisLabel"
                                + ",PriceAnalysisValue"
                                + ",CreateTime"
                                + ",UpdateTime"
                                + ",State"
                                + ",City"
                                + ",Is_new"
                                + ") ";
                            sql2 += "values("
                                + hid.ToString()
                                + ",'" + d_EstateName + "'"
                                + ",'" + d_EstateAddress + "'"
                                + ",'" + d_EstatePic + "'"
                                + ",'" + d_SaleStateTag + "'"
                                + ",'" + d_EstateTag + "'"
                                + ",'" + d_RoomType + "'"
                                + ",'" + d_OpenTime + "'"
                                + ",'" + d_LastPriceLabel + "'"
                                + ",'" + d_LastPriceValue + "'"
                                + ",'" + d_TrendPriceLabel + "'"
                                + ",'" + d_TrendPriceValue + "'"
                                + ",'" + d_RegionLastPriceLabel + "'"
                                + ",'" + d_RegionLastPriceValue + "'"
                                + "," + d_HighPriceEstateCount
                                + "," + d_LowPriceEstateCount
                                + "," + d_FlatPriceEstateCount
                                + ",'" + d_HighPriceEstate + "'"
                                + ",'" + d_LowPriceEstate + "'"
                                + ",'" + d_FlatPriceEstate + "'"
                                + ",'" + d_PriceAnalysisLabel + "'"
                                + ",'" + d_PriceAnalysisValue + "'"
                                + ",'" + d_CreateTime + "'"
                                + ",'" + d_UpdateTime + "'"
                                + "," + d_State
                                + ",'" + cityCode + "'"
                                + "," + d_is_new
                                + ");";
                            Console.WriteLine(sql2);

                            SimpleDataHelper.Excsql(SimpleDataHelper.MSConnectionString, sql2);

                            DataTable dt = new DataTable();
                            ;          dt.Columns.Add("hid", typeof(Int32));
                            dt.Columns.Add("EstateName", typeof(string));
                            dt.Columns.Add("MarkDate", typeof(string));
                            dt.Columns.Add("BeginPrice", typeof(double));
                            dt.Columns.Add("AvgPrice", typeof(double));
                            dt.Columns.Add("MaxPrice", typeof(double));
                            dt.Columns.Add("MinTotalPrice", typeof(double));
                            dt.Columns.Add("MaxTotalPrice", typeof(double));
                            dt.Columns.Add("PriceDescription", typeof(string));
                            dt.Columns.Add("CreateTime", typeof(string));
                            dt.Columns.Add("UpdateTime", typeof(string));
                            dt.Columns.Add("State", typeof(Int32));
                            dt.Columns.Add("City", typeof(string));

                            foreach (JObject item in house_price_trend["all_price"])
                            {
                                string d_MarkDate = (string)item["price_time"];
                                double d_BeginPrice = (double)item["price_min"];
                                double d_AvgPrice = (double)item["price_avg"];
                                double d_MaxPrice = (double)item["price_max"];
                                double d_MinTotalPrice = (double)item["price_sum_min"];
                                double d_MaxTotalPrice = (double)item["price_sum_max"];
                                string d_PriceDescription = (string)item["price_show"];

                                System.Data.DataRow dataRow;
                                dataRow = dt.NewRow();
                                dataRow["hid"] = hid;
                                dataRow["EstateName"] = d_EstateName;
                                dataRow["MarkDate"] = SimpleDataHelper.GetTime(d_MarkDate);
                                dataRow["BeginPrice"] = d_BeginPrice;
                                dataRow["AvgPrice"] = d_AvgPrice;
                                dataRow["MaxPrice"] = d_MaxPrice;
                                dataRow["MinTotalPrice"] = d_MinTotalPrice;
                                dataRow["MaxTotalPrice"] = d_MaxTotalPrice;
                                dataRow["PriceDescription"] = d_PriceDescription;
                                dataRow["CreateTime"] = d_CreateTime;
                                dataRow["UpdateTime"] = d_UpdateTime;
                                dataRow["State"] = d_State;
                                dataRow["City"] = cityCode;

                                dt.Rows.Add(dataRow);

                            }

                            SimpleDataHelper.SqlBCP(SimpleDataHelper.MSConnectionString, dt, "TB_ESTATE_FEATURES_PRICELIST");

                            sql2 = @"insert into TB_ESTATE_FEATURES_PRICETREND (hid,EstateName,Price,PriceDate,CreateTime,UpdateTime,State,City)
select " + hid + @",'" + d_EstateName + @"',b.AvgPrice,a.md,'" + d_CreateTime + @"','" + d_UpdateTime + @"'," + d_State + @",'" + cityCode + @"' from (
SELECT [hid],[City],ym,max([MarkDate]) as md
FROM (
SELECT [hid],[City],DATEPART(YEAR, [MarkDate])*100+DATEPART(MONTH, [MarkDate]) as ym,[MarkDate] FROM [TB_ESTATE_FEATURES_PRICELIST] where [AvgPrice]>0 and hid=" + hid + @" and City='" + cityCode + @"'
) as z
group by hid,city,ym
) as a left join [TB_ESTATE_FEATURES_PRICELIST] as b on a.hid=b.hid and a.City=b.City and a.md=b.MarkDate;
";
                            Console.WriteLine(sql2);

                            SimpleDataHelper.Excsql(SimpleDataHelper.MSConnectionString, sql2);

                            sql2 = @"update LEJU_ESTATE_BASEINFO set is_new=0 where is_new=1 and hid=" + hid.ToString() + " and city='" + cityCode + "'";
                            Console.WriteLine(sql2);

                            SimpleDataHelper.Excsql(SimpleDataHelper.MSConnectionString, sql2);

                        }
                        catch (Exception ex)
                        {
                            WriteLog("解析过程出错|"+ex.Message);
                        }
                    }



                }
            }
            catch (Exception ex)
            {
                WriteLog("尚未解析|" + ex.Message);

            }


        }

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


    }
}
