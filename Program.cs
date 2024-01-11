using ClosedXML;
using ClosedXML.Excel;
using ClosedXML.Graphics;
using MDBOCBusinessLogic.Maestros;
using MDDBCDataAccess.Maestros;
using MDDTOEntities.Finanzas;
using MDDTOEntities.Maestros;
using MDDTOEntities.Ventas;
using MigracionData.Modelo;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using OfficeOpenXml;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.Remoting.Metadata.W3cXsd2001;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
 
namespace MigracionData
{
     class Program
    {
        //static string sEsquema = "";
        //static string sEsquema = "\"PODS_LM\".\"ODS_RIESGOS\".";
        static string sEsquema =  System.Configuration.ConfigurationManager.AppSettings["param1"] ; 
        //ODS_RIESGOS_TBS
        static string sConexion = ConfigurationManager.ConnectionStrings["cnxOracle"].ConnectionString;

        static string sRutaExcel = System.Configuration.ConfigurationManager.AppSettings["RutaExcel"];

        static string sRutaExcelExport = System.Configuration.ConfigurationManager.AppSettings["RutaExcelExport"];

        static void fn_GenerarExcelActualizado()
        {
            try
            {
                DBCGeneric oDbGeneric = new DBCGeneric();

                DataTable oEECC = oDbGeneric.fn_ObtenerResultado("Pa_Pendientes_EECC_Excel_Listar2");

                using (XLWorkbook wb = new XLWorkbook())
                {

                    wb.Worksheets.Add(oEECC, "Pa_Pendientes_EECC");

                    //string Ruta = @"C:\\Users\\RPA_Entel-PE11\\Entel Peru S.A\\EntelDrive Canal Indirecto y Fraudes - EECC_WEB_MAY\\";
                    //string Ruta = @"C:\Users\RPA_Entel-PE11\Entel Peru S.A\EntelDrive Canal Indirecto y Fraudes - Documentos\MACROS_CI\MONITOR SOCIOS\EECC_WEB_MAY\\";
                    string fileName =sRutaExcelExport;

                    //fileName = Ruta  + fileName ;

                    Console.WriteLine(fileName);

                    if (!System.IO.File.Exists(fileName))
                    {
                        wb.SaveAs(fileName);
                    }
                    else
                    {
                        System.IO.File.Delete(fileName);
                        wb.SaveAs(fileName);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                //Console.ReadLine();
            }

        }


        public static RespuestaPostEnvio fn_AnulacionPedido(string pidOperacion, string pNumeroCupon)
        {
            RespuestaPostEnvio oRespuestaPostEnvio = new RespuestaPostEnvio();
            string JSON = "";
            //JSON = "{""canal"":""OP"",""codConvenio"":13003,""codOperacion"":2010,""empresaOrigen"":""Entel"",""fechaVencimiento"":""2021-05-30"",""idCliente"":""TESTDESARROLLO"",""idOperacion"":2022,""importe"":1,""moneda"":""PEN"",""nombreCliente"":""Contoso""}";
            string sResultado = "";

            CuponInput oCuponInput = new CuponInput();
            oCuponInput.email_token = "fasty.entel@insolutions.pe";
            oCuponInput.clave_token = "3Qv6M8#@w$N97Kqr"; 
            oCuponInput.Api_token = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/users/login";

        //oCuponInput.Api_Cupon = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas";
        //oCuponInput.Api_Patch = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas/" + pNumeroCupon;

                                    
            oCuponInput.Api_Cupon = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas";
            oCuponInput.Api_Patch = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas/" + pNumeroCupon;

            oCuponInput.canal = "OP";
            oCuponInput.codConvenio = "13002"; //"13003";
            oCuponInput.codOperacion = "3010"; 
            oCuponInput.empresaOrigen = "ENTEL";//AMBOS
            oCuponInput.idOperacion = pidOperacion;//AMBOS

            JSON = JsonConvert.SerializeObject(oCuponInput);

            Console.WriteLine(JSON);
            //Console.ReadLine();
            var url = "http://172.30.30.57/apicupon/api/Cupon/fn_AnularCupon";
            //var request = (HttpWebRequest)WebRequest.Create(url);
            var request = (HttpWebRequest)WebRequest.Create(url);

            request.Method = "POST";
            request.ContentType = "application/json";
            request.Accept = "application/json";


            WebProxy proxy = new WebProxy();
            proxy.UseDefaultCredentials = true;
            //request.Proxy = proxy;

            request.Proxy = proxy;


            using (var streamWriter = new StreamWriter(request.GetRequestStream()))
            {
                streamWriter.Write(JSON);
                streamWriter.Flush();
                streamWriter.Close();
            }

            using (WebResponse response = request.GetResponse())
            {
                using (Stream strReader = response.GetResponseStream())
                {
                    if (strReader == null) return oRespuestaPostEnvio;
                    using (StreamReader objReader = new StreamReader(strReader))
                    {
                        string responseBody = objReader.ReadToEnd();
                        oRespuestaPostEnvio = JsonConvert.DeserializeObject<RespuestaPostEnvio>(responseBody);


                    }
                }
            }
            oRespuestaPostEnvio.JSON = JSON;

            return oRespuestaPostEnvio;
        }

        public static void fn_AnularCupon()
        {
            string sIDPedidoDetalle = "";
            string sIDOperacion = "";
            string sNumeroCupon = "";
            string sUsuarioAnulo = "";

            try
            {

            DataTable oObj_EECC = new DataTable();
            string sQueryOracle = "SELECT * from V_PEDIDODETALLE_LISTAR2 where ( (CODIGOESTADOPEDIDO in ('ANU')  and  NUMEROCUPON <> ' ')  " + 
                "AND ESTADOCUPON IN('PENDIENTE', 'GENERADO') )";

            Console.WriteLine("sQueryOracle: " + sQueryOracle);
                
                using (OracleDataAdapter adp = new OracleDataAdapter(sQueryOracle, sConexion))
            {
                //a Datatable to store records 
                //now im going to fetch data
                adp.Fill(oObj_EECC);//all the data in OracleAdapter will be filled into Datatable 
            }

            Console.WriteLine("cupones por ANULAR: " + oObj_EECC.Rows.Count);
            //Console.ReadLine();

                foreach (DataRow oRows in oObj_EECC.Rows)
            {
                sIDPedidoDetalle = oRows["IDPEDIDODETALLE"].ToString();
                sIDOperacion = oRows["IDOPERACION"].ToString();
                    sNumeroCupon = oRows["NUMEROCUPON"].ToString();
                    RespuestaPostEnvio oRespuestaPostEnvio= fn_AnulacionPedido(sIDOperacion, sNumeroCupon);

                BOCGeneric oBoGeneric=new BOCGeneric();

                Console.WriteLine("oRespuestaPostEnvio.message " + oRespuestaPostEnvio.message);

                if (oRespuestaPostEnvio.message != null)
                {

                    

                    fn_Registrar("UPDATE " + sEsquema + "PedidoDetalle SET " +
                    //"FECHAANULACIONPEDIDO='" + DateTime.Now + "' , " +
                    //"USUARIOANULO='" + sUsuario + "' , " +
                    "CODIGOESTADOCUPON='" + "ANU" + "' , " +
                    "ESTADOCUPON='" + "ANULADO" + "' ," +
                    "MENSAJEAPIANULACION='" + oRespuestaPostEnvio.message + "' ," +
                    "JSONANULACION='" + oRespuestaPostEnvio.JSON + "' " +
                    "WHERE PKID ='" + sIDPedidoDetalle + "'  ");


                }

                else
                {
                    fn_Registrar("UPDATE " + sEsquema + "PedidoDetalle SET " +
            "MENSAJEAPIANULACION='" + oRespuestaPostEnvio.message + "' ," +
            "JSONANULACION='" + oRespuestaPostEnvio.JSON + "' " +
            "WHERE PKID ='" +
            sIDPedidoDetalle + "'  ");


                }
            }


            }
            catch (Exception EX)
            {
                
                Console.WriteLine(EX.Message);
                //Console.ReadLine();
            }
        }


        public static RespuestaPostEnvioMultipleFiltro fn_obtenerCobranzasLote_OracleEECC(string ResToken)
        {
            //string Api_envio = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas/lote/obtenerCobranzas";
            string Api_envio = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas/lote/obtenerCobranzas";

            RespuestaPostEnvioMultipleFiltro oResultado = new RespuestaPostEnvioMultipleFiltro();
            string JSON = "";
            //JSON = "{""canal"":""OP"",""codConvenio"":13003,""codOperacion"":2010,""empresaOrigen"":""Entel"",""fechaVencimiento"":""2021-05-30"",""idCliente"":""TESTDESARROLLO"",""idOperacion"":2022,""importe"":1,""moneda"":""PEN"",""nombreCliente"":""Contoso""}";
            string sResultado = "";

            DataTable oObj_EECC = new DataTable();
            string sQueryOracle = "select PKID , IDEECC, ESTADO AS CODIGOESTADOPEDIDO, ESTADO ESTADOCUPON, PKID IDOPERACION, NUMEROCUPON  from VIEW_ESTADOCUENTACUPON_LISTAR where (ESTADO in ('PENDIENTE')   and  NUMEROCUPON <> ' ')";
            //string sQueryOracle = "select PKID , IDEECC, ESTADO AS CODIGOESTADOPEDIDO, ESTADO ESTADOCUPON, PKID IDOPERACION, NUMEROCUPON  from VIEW_ESTADOCUENTACUPON_LISTAR where NUMEROCUPON = 'C415830'";
            Console.WriteLine("sQueryOracle: " + sQueryOracle);

            using (OracleDataAdapter adp = new OracleDataAdapter(sQueryOracle, sConexion))
            {
                //a Datatable to store records 
                //now im going to fetch data
                adp.Fill(oObj_EECC);//all the data in OracleAdapter will be filled into Datatable 
            }

            Console.WriteLine("cupones pendiente: " + oObj_EECC.Rows.Count);
                //Thread.Sleep(3000);
        
            string NRO_CUPON = "";
            string[] arrCupones = new string[1];

            int xCCupo = 0;
            string ID_SOCIO = "";
            string NroFactura_2 = "";
            string sIDOperacion_2 = "";
            string sQuery = "";


            foreach (DataRow oRows in oObj_EECC.Rows)
            {
                xCCupo = 0;
                EnvioPostLote oEnvioPost = new EnvioPostLote();

                //oEnvioPost.canal = "OP";//pcanal; CANAL DE OPERACIONES
                //oEnvioPost.codConvenio = "13003"; //convenio ISOLUTIONS CON ENTEL 13004 PROD 13002 PRODUCCION
                //oEnvioPost.codOperacion = "1020"; // AMBOS
                //oEnvioPost.empresaOrigen = "ENTEL";//AMBOS

                oEnvioPost.canal = "OP";//pcanal; CANAL DE OPERACIONES
                oEnvioPost.codConvenio = "13002"; //convenio ISOLUTIONS CON ENTEL 13004 PROD 13002 PRODUCCION
                oEnvioPost.codOperacion = "1020"; // AMBOS
                oEnvioPost.empresaOrigen = "ENTEL";//AMBOS 
                //sIDOperacion_2 = oRows["IDOPERACION"].ToString();

                Random rnd = new Random();
                int iIDOperacion = rnd.Next(99999999);     // creates a number between 0 and 51
                sIDOperacion_2 = iIDOperacion.ToString("00000000");

                oEnvioPost.idOperacion = sIDOperacion_2;//oRows["IDOPERACION"].ToString();//AMBOS                 
                NRO_CUPON = oRows["NUMEROCUPON"].ToString();
                arrCupones[xCCupo] = NRO_CUPON;


                oEnvioPost.listadoCodigoCobranza = arrCupones;

                JSON = JsonConvert.SerializeObject(oEnvioPost);


                Console.WriteLine("ENTRO: XXX" );

                Console.WriteLine("JSON CUPON: " + JSON);


                Console.WriteLine("****JSON ENVIO ******" + JSON);
                Console.WriteLine("****API REST ENVIO ******" + Api_envio);
                Console.WriteLine("****Enviando Serializacion con TOKEN....******" + Api_envio);

                Console.WriteLine("****NRO_CUPON ******" + NRO_CUPON);

                //Thread.Sleep(4000);
                //Console.ReadLine();

                using (var stringContent = new
                 StringContent(JSON,
             System.Text.Encoding.UTF8, "application/json"))
                using (var client = new HttpClient())
                {

                    client.DefaultRequestHeaders.Authorization = new
                        AuthenticationHeaderValue("Bearer", ResToken);

                    Console.WriteLine("****Obteniendo respuesta POST Async....******" + Api_envio);

                    var response = client.PostAsync(Api_envio, stringContent);

                    //var result = await response.Content.ReadAsStringAsync();
                    var result = response.Result;
                    sResultado = result.ReasonPhrase;
                    var contex = response.Result.Content.ReadAsStringAsync();

                    oResultado = JsonConvert.DeserializeObject<RespuestaPostEnvioMultipleFiltro>(contex.Result.ToString());
                    

                    if (oResultado.data != null)
                    {
                        foreach (var pRes in oResultado.data)
                        {

                            string sEstado = pRes.estado.ToUpper();
                            string sCodigoEstado = "";


                            if (sEstado == "V")
                            {
                                sEstado = "PENDIENTE";
                                sCodigoEstado = "PEND";
                            }
                            else if (sEstado == "P")
                            {
                                sEstado = "CANCELADO";
                                sCodigoEstado = "CANC";

                            }
                            else if (sEstado == "E")
                            {
                                sEstado = "ANULADO";
                                sCodigoEstado = "ANU";
                            }
                            Console.WriteLine("ENTRO: " + sEstado);
                            Console.WriteLine("ESTADO: " + sEstado);
                            Console.WriteLine("idTransaccionRegistro: " + pRes.idTransaccionRegistro);
                            Console.WriteLine("descripcionCobranza: " + pRes.descripcionCobranza);
                            Console.WriteLine("NRO_CUPON: " + NRO_CUPON);
                            Console.WriteLine("sIDOperacion_2: " + sIDOperacion_2);

                            //Thread.Sleep(4000);

                            //Console.ReadLine();
                            sQuery = "";
                            if (sEstado != "")
                            {
                                sQuery = "UPDATE " + sEsquema + "\"ESTADOCUENTACUPON\" SET   "
                                    + "ESTADO ='" + sEstado + "'  ," +
                                       "FECHAREGISTRO ='" + DateTime.Now.ToShortDateString() + "' " +
                                       "where PKID='" + oRows["PKID"].ToString() + "' ";

                                fn_Registrar(sQuery);

                                Console.WriteLine("sQuery: " + sQuery);

                                Console.WriteLine("sEstado" + sEstado);
                                //Thread.Sleep(4000);
                                if (sEstado == "CANCELADO")
                                {
                                    Console.WriteLine("ENTRO EN PAGADO");


                                    string sBanco = pRes.nombreProveedorPago;
                                    string sPKIDEECCPago = "";string sUltimoG = "";
                                    string sIDOperacion =
            DateTime.Now.ToString("yy") + "" +
            DateTime.Now.Month.ToString("00") + "" +
            DateTime.Now.Day.ToString("00");

                                    Console.WriteLine("sBanco " + sBanco);
                                    Console.WriteLine("sIDOperacion " + sIDOperacion);

                                    DataTable oCorrelativo = fn_ObtenerResultado("Select ( MAX(SUBSTR(DIA,7,6)) +1 )ULTIMO from " +
                                 sEsquema + "\"CORRELATIVO_EECC\"  WHERE VALOR ='" + sIDOperacion + "' and TIPO='PED' ");

                                    foreach (DataRow oRes in oCorrelativo.Rows)
                                    {
                                        sUltimoG = oRes["ULTIMO"].ToString();
                                    }

                                    if (sUltimoG.Length == 0)
                                    {
                                        DataTable oCorrelativoG = fn_ObtenerResultado("Select ( MAX(IDENTIFICADOR) +1 )ULTIMO from " +
                                     sEsquema + "\"CORRELATIVO_EECC\"  ");

                                        foreach (DataRow oRes in oCorrelativoG.Rows)
                                        {
                                            sUltimoG = oRes["ULTIMO"].ToString();
                                        }

                                        string sCorrelativoAMD = sIDOperacion + "0001";

                                        fn_Registrar("INSERT INTO " + sEsquema + "\"CORRELATIVO_EECC\" (IDENTIFICADOR,DESCRIPCION,CORRELATIVO,VALOR,DIA,TIPO) " +
                                                " VALUES('" + sUltimoG + "','" + sIDOperacion + "','" + sCorrelativoAMD + "','" + sIDOperacion + "','" + sCorrelativoAMD + "','" + "PED" + "') ");

                                        //fn_Registrar("UPDATE " + sEsquema + "\"CORRELATIVO_EECC\" SET CORRELATIVO =" + sCorrelativoAMD+ " WHERE VALOR ='" + sIDOperacion + "' and TIPO='PED' ");
                                        sPKIDEECCPago = sCorrelativoAMD;
                                    }
                                    else
                                    {
                                        /*foreach (DataRow oRes in oCorrelativo.Rows)
                                        {
                                            sUltimoG =  (Convert.ToInt32( oRes["ULTIMO"]) +1 ).ToString();
                                        }*/

                                        string sIDOperacion2 = sIDOperacion + "" + Convert.ToInt32(sUltimoG).ToString("0000");

                                        fn_Registrar("UPDATE " + sEsquema + "\"CORRELATIVO_EECC\" SET CORRELATIVO =" + sUltimoG + ", DIA='" + sIDOperacion2 + "' WHERE  VALOR ='" + sIDOperacion + "' and TIPO='PED' ");
                                        sPKIDEECCPago = sIDOperacion2;

                                    }


                                    //sPKIDEECCPago = sCorrelativoAMD;


                                    Console.WriteLine(sPKIDEECCPago);

                                    

                                    if (sBanco=="BCP")
                                {
                                    sBanco = "198"; // BCP
                                }
                                else if (sBanco == "BBVA")
                                {
                                    sBanco = "199"; // BCP
                                }
                                else
                                {
                                    sBanco = "0";
                                }

                                    Console.WriteLine("pRes.idOperacion " + pRes.idOperacion);
                                    Console.WriteLine("sBanco " + sBanco);
                                    Console.WriteLine("sPKIDEECCPago " + sPKIDEECCPago);
                                    Console.WriteLine("oRows[\"IDEECC\"].ToString()  " + oRows["IDEECC"].ToString());
                                    Console.WriteLine("ENTRO EN PAGADO");
                                    Console.WriteLine(" " + pRes.fechaProcesoPago.Substring(0, 10));
                                    Console.WriteLine(" " + pRes.fechaProcesoPago);
                                    fn_Registrar("INSERT INTO " + sEsquema + "\"ESTADOCUENTAPAGO\" (PKID, IDEECC,FECHAPAGO,OBSERVACION,NUMEROOPERACION,USUARIOREGISTRO,FECHAREGISTRO,AUDITORIA,IDCUPON,IDESTADO,IMPORTE,BANCO,IDTIPOMEDIOPAGO ) VALUES( " +
sPKIDEECCPago +//PKID
", '" + oRows["IDEECC"].ToString() + "'" + // 06/06/2023 03:57:20 p. m.
", '" + Convert.ToDateTime(pRes.fechaProcesoPago.Substring(0,10) .ToString().Replace("-","/")).ToShortDateString() + "'" +
//", '" + ddlTipoFacturacion.SelectedValue + "'" +
" ,'" + "API_AUTOMATICO" + "'" +
" ,'" + "" + "'" +
" ,'" + "API_AUTOMATICO" + "'" +
" ,'" + DateTime.Now.ToShortDateString() + "'" +
" ,'" + "0" + "'" +
" ,'" + oRows["PKID"].ToString() + "'" +
" ,'" + "195" + "'" +
" ,'" + pRes.importe.Replace(".", ",") + "'" +
" ,'" + sBanco + "'" +
" ,'" + "185" + "' )"
);
                                    Console.WriteLine("salio EN PAGADO");

                                    Console.WriteLine("SE INSERTO CORRECTAMENTE ESTADOCUENTAPAGO");
                                    //fn_ActualizarExcel(sBase, sQueryExcel);

                                    Console.WriteLine(sQuery);
                                    //Thread.Sleep(3000);
                                    //Console.ReadLine();

                                    //Environment.Exit(0);

                                      }

                            }



                            Console.WriteLine("****sQuery Update******" + sQuery);
                        }
                    }
                    Console.WriteLine("****Transformando Respuesta a DeserializeObject RespuestaPostEnvio ******");

                    /*
                    Console.WriteLine("****transaccionId******" + oResultado.transaccionId);
                    Console.WriteLine("****id******" + oResultado.id);
                    Console.WriteLine("****fechaRegistro******" + oResultado.fechaRegistro);*/
                    Console.WriteLine("****message******" + oResultado.message);
                    xCCupo++;
                }
                //Console.ReadLine();
            }


            Console.WriteLine("SALIENDO");
            //Thread.Sleep(3000);

            return oResultado;
        }

        public static RespuestaPostEnvioMultipleFiltro fn_obtenerCobranzasLote_Oracle(string ResToken)
        {
            //string Api_envio = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas/lote/obtenerCobranzas";
            string Api_envio = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas/lote/obtenerCobranzas";

            RespuestaPostEnvioMultipleFiltro oResultado = new RespuestaPostEnvioMultipleFiltro();
            string JSON = "";
            //JSON = "{""canal"":""OP"",""codConvenio"":13003,""codOperacion"":2010,""empresaOrigen"":""Entel"",""fechaVencimiento"":""2021-05-30"",""idCliente"":""TESTDESARROLLO"",""idOperacion"":2022,""importe"":1,""moneda"":""PEN"",""nombreCliente"":""Contoso""}";
            string sResultado = "";
             
            DataTable oObj_EECC = new DataTable();
            string sQueryOracle = "SELECT CODIGOESTADOPEDIDO,ESTADOCUPON,IDOPERACION,IDOPERACION,NUMEROCUPON,IDPEDIDODETALLE from V_PEDIDODETALLE_LISTAR2 where ( (CODIGOESTADOPEDIDO in ('REG','ANU','APT')   and  NUMEROCUPON <> ' ') AND ESTADOCUPON IN('PENDIENTE', 'GENERADO') )";

            Console.WriteLine("sQueryOracle: " + sQueryOracle);

            using (OracleDataAdapter adp = new OracleDataAdapter(sQueryOracle,sConexion))
            {
                //a Datatable to store records 
                //now im going to fetch data
                adp.Fill(oObj_EECC);//all the data in OracleAdapter will be filled into Datatable 
            }

            Console.WriteLine("cupones pendiente: " + oObj_EECC.Rows.Count);

            
            string NRO_CUPON = "";
            string[] arrCupones = new string[1];

            int xCCupo = 0;
            string ID_SOCIO = "";
            string NroFactura_2 = "";
            string sIDOperacion_2 = "";
            string sQuery = "";
            foreach (DataRow oRows in oObj_EECC.Rows)
            {
                xCCupo = 0;
                EnvioPostLote oEnvioPost = new EnvioPostLote();

                //oEnvioPost.canal = "OP";//pcanal; CANAL DE OPERACIONES
                //oEnvioPost.codConvenio = "13003"; //convenio ISOLUTIONS CON ENTEL 13004 PROD 13002 PRODUCCION
                //oEnvioPost.codOperacion = "1020"; // AMBOS
                //oEnvioPost.empresaOrigen = "ENTEL";//AMBOS

                oEnvioPost.canal = "OP";//pcanal; CANAL DE OPERACIONES
                oEnvioPost.codConvenio = "13002"; //convenio ISOLUTIONS CON ENTEL 13004 PROD 13002 PRODUCCION
                oEnvioPost.codOperacion = "1020"; // AMBOS
                oEnvioPost.empresaOrigen = "ENTEL";//AMBOS 
                sIDOperacion_2 = oRows["IDOPERACION"].ToString();
                oEnvioPost.idOperacion = oRows["IDOPERACION"].ToString();//AMBOS                 
                NRO_CUPON = oRows["NUMEROCUPON"].ToString();
                arrCupones[xCCupo] = NRO_CUPON;
                 

                oEnvioPost.listadoCodigoCobranza = arrCupones;
                 
                JSON = JsonConvert.SerializeObject(oEnvioPost);

                Console.WriteLine("JSON CUPON: "+ JSON);

                
                Console.WriteLine("****JSON ENVIO ******" + JSON);
                Console.WriteLine("****API REST ENVIO ******" + Api_envio);
                Console.WriteLine("****Enviando Serializacion con TOKEN....******" + Api_envio);

                Console.WriteLine("****NRO_CUPON ******" + NRO_CUPON);
                //Console.ReadLine();

                using (var stringContent = new
                 StringContent(JSON,
             System.Text.Encoding.UTF8, "application/json"))
                using (var client = new HttpClient())
                {

                    client.DefaultRequestHeaders.Authorization = new
                        AuthenticationHeaderValue("Bearer", ResToken);

                    Console.WriteLine("****Obteniendo respuesta POST Async....******" + Api_envio);

                    var response = client.PostAsync(Api_envio, stringContent);

                    //var result = await response.Content.ReadAsStringAsync();
                    var result = response.Result;
                    sResultado = result.ReasonPhrase;
                    var contex = response.Result.Content.ReadAsStringAsync();

                    oResultado = JsonConvert.DeserializeObject<RespuestaPostEnvioMultipleFiltro>(contex.Result.ToString());

                    if (oResultado.data != null)
                    {
                        foreach (var pRes in oResultado.data)
                        {
                            
                            string sEstado = pRes.estado.ToUpper();
                            string sCodigoEstado = "";
                             
                            
                            if (sEstado == "V") {  
                                sEstado = "PENDIENTE";
                                sCodigoEstado = "PEND";
                            }
                            else if (sEstado == "P") {  
                                sEstado = "CANCELADO";
                                sCodigoEstado = "CANC";

                            }
                            else if (sEstado == "E") {  
                                sEstado = "ANULADO";
                                sCodigoEstado = "ANU";
                            }

                            Console.WriteLine("ESTADO: " + sEstado);
                            Console.WriteLine("idTransaccionRegistro: " + pRes.idTransaccionRegistro);
                            Console.WriteLine("descripcionCobranza: " + pRes.descripcionCobranza);
                            Console.WriteLine("NRO_CUPON: " + NRO_CUPON);
                            Console.WriteLine("sIDOperacion_2: " + sIDOperacion_2);

                            //Console.ReadLine();
                            sQuery = "";
                            if (sEstado != "") {   

                                sQuery = "UPDATE " + sEsquema + "\"PEDIDODETALLE\" SET  CODIGOESTADOCUPON = '"+ sCodigoEstado + "', "
                                    + "ESTADOCUPON ='" + sEstado + "'  ,"
                                    + "ULTIMAFECHAACTUALIZACIONCUPON ='" + DateTime.Now + "'  ," +
   "CUPON_BANCO ='" + pRes.nombreProveedorPago + "'  ," +
   "CUPON_IMPORTE_ABONADO = '" + "S/" + pRes.importe + "'  ," +
   "CUPON_NRO_OP = '" + pRes.idOperacion + "'  ," +
   "CUPON_FECHA_RPTA = '" + DateTime.Now.ToShortDateString() + "'  ," +
   "CUPON_HORA_RPTA = '" + DateTime.Now.ToString("hh:mm:ss") + "'  ," +
   "CUPON_F_ABONO = '" + Convert.ToDateTime(pRes.fechaProcesoPago).ToString() + "'  ," + //.ToString("dd-MM-yyyy hh:mm:ss") + "'  " +
   "CUPON_CANAL_PAGO = '" + pRes.canal + "'  " + //.ToString("dd-MM-yyyy hh:mm:ss") + "'  " +

   "where PKID='" + oRows["IDPEDIDODETALLE"].ToString() + "' ";

                                // "UPDATE " + sEsquema + "\"PEDIDODETALLE\" SET  TRANSFERIDOEECC='1' " +   " WHERE   PKID='" + oRows["IDPEDIDODETALLE"].ToString() + "' "


                                fn_Registrar(sQuery);
                                //fn_ActualizarExcel(sBase, sQueryExcel);
                                
                                Console.WriteLine(sQuery);
                                //Console.ReadLine();

                                //Environment.Exit(0);


                            }



                            Console.WriteLine("****sQuery Update******" + sQuery);
                        }
                    }
                    Console.WriteLine("****Transformando Respuesta a DeserializeObject RespuestaPostEnvio ******");

                    /*
                    Console.WriteLine("****transaccionId******" + oResultado.transaccionId);
                    Console.WriteLine("****id******" + oResultado.id);
                    Console.WriteLine("****fechaRegistro******" + oResultado.fechaRegistro);*/
                    Console.WriteLine("****message******" + oResultado.message);
                    xCCupo++;
                }
                //Console.ReadLine();
            }
            return oResultado;
        }


        public static RespuestaPostEnvioMultipleFiltro fn_obtenerCobranzasLote_Excel_Unitario(string ResToken, 
            string pNumeroCupon, string Api_envio_consultaCupon,string pcodConvenio)
        {

            //string Api_envio = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas/lote/obtenerCobranzas";

            RespuestaPostEnvioMultipleFiltro oResultado = new RespuestaPostEnvioMultipleFiltro();
            string JSON = "";
            string sResultado = "";

            //string sQueryExcel = "select * from [Pendientes_EECC$] where ESTADO LIKE '%PENDIENTE%' ";
            //DataTable oObj_EECC = fn_LeerExcel(sRutaExcel, "Pendientes_EECC", sQueryExcel);
            DataTable oObj_EECC = null;
            string NRO_CUPON = "";
            string[] arrCupones = new string[1];

            int xCCupo = 0;
            string ID_SOCIO = "";
            string NroFactura_2 = "";
            string sIDOperacion_2 = "";

            DBCGeneric oDbGeneric = new DBCGeneric();


            //fn_CargarTabla(oObj_EECC);

            oDbGeneric = new DBCGeneric();

            //oObj_EECC = oDbGeneric.fn_ObtenerResultado("Pa_Pendientes_EECC_Excel_Listar");

            //foreach (DataRow oRows in oObj_EECC.Rows)
            //{
            xCCupo = 0;
            EnvioPostLote oEnvioPost = new EnvioPostLote();

            //oEnvioPost.canal = "OP";//pcanal; CANAL DE OPERACIONES
            //oEnvioPost.codConvenio = "13003"; //convenio ISOLUTIONS CON ENTEL 13004 PROD 13002 PRODUCCION
            //oEnvioPost.codOperacion = "1020"; // AMBOS
            //oEnvioPost.empresaOrigen = "ENTEL";//AMBOS

            oEnvioPost.canal = "OP";//pcanal; CANAL DE OPERACIONES
            oEnvioPost.codConvenio = pcodConvenio;//"13002"; //convenio ISOLUTIONS CON ENTEL 13004 PROD 13002 PRODUCCION
            oEnvioPost.codOperacion = "1020"; // AMBOS
            oEnvioPost.empresaOrigen = "ENTEL";//AMBOS


            //NroFactura_2 = oRows["NRO_FACTURA"].ToString();

            Random rnd = new Random();
            int iIDOperacion = rnd.Next(99999999);     // creates a number between 0 and 51
            sIDOperacion_2 = iIDOperacion.ToString("00000000");

            /*
            foreach (DataRow oRes
                            in fn_ObtenerResultado("Select ( MAX(CORRELATIVO) +1 )ULTIMO from " +
                            sEsquema + "\"CORRELATIVO_EECC\"  WHERE VALOR ='" + DateTime.Now.Year
                            + "'").Rows)
            {
                sIDOperacion_2 = ((DateTime.Now.Year - 2000)
                    + Convert.ToInt32(oRes["ULTIMO"]).ToString("0000000"));

                fn_Registrar("UPDATE " + sEsquema + "\"CORRELATIVO_EECC\" SET CORRELATIVO =" +
                    Convert.ToInt32(oRes["ULTIMO"]) + " WHERE VALOR ='" + DateTime.Now.Year + "'");
                fn_Registrar("INSERT INTO " + sEsquema + "\"EQUIVALENCIA_EECC\" (IDENTIFICADOR,ORIGEN,FECHAREGISTRO) VALUES( " +
                    sIDOperacion_2 + " ,'" + NroFactura_2 + "', '" + DateTime.Now + "' ) ");
            }
            */

                oEnvioPost.idOperacion = sIDOperacion_2;//AMBOS

            //oEnvioPost.idOperacion = "230000015";
            //oEnvioPost.idOperacion = sIDOperacion_2;//oRows["NRO_FACTURA"].ToString(); //"12312";

                NRO_CUPON = pNumeroCupon;
                arrCupones[xCCupo] = NRO_CUPON;
            // ID_SOCIO = oRows["ID_SOCIO"].ToString();

                //arrCupones = new string[oObj_EECC.Rows.Count];
                //arrCupones = new string[1];
                oEnvioPost.listadoCodigoCobranza = arrCupones;

                /*
                oEnvioPost.listadoCodigoCobranza = new string[2];
                oEnvioPost.listadoCodigoCobranza[0] = "C403339";
                oEnvioPost.listadoCodigoCobranza[1] = "C403340";
                */
                JSON = JsonConvert.SerializeObject(oEnvioPost);

                Console.WriteLine("****JSON ENVIO ******" + JSON);
                Console.WriteLine("****API REST ENVIO ******" + Api_envio_consultaCupon);
                Console.WriteLine("****Enviando Serializacion con TOKEN....******" + Api_envio_consultaCupon);

                using (var stringContent = new
                 StringContent(JSON,
             System.Text.Encoding.UTF8, "application/json"))
                using (var client = new HttpClient())
                {

                    client.DefaultRequestHeaders.Authorization = new
                        AuthenticationHeaderValue("Bearer", ResToken);

                    Console.WriteLine("****Obteniendo respuesta POST Async....******" + Api_envio_consultaCupon);

                    var response = client.PostAsync(Api_envio_consultaCupon, stringContent);

                    //var result = await response.Content.ReadAsStringAsync();
                    var result = response.Result;
                    sResultado = result.ReasonPhrase;
                    var contex = response.Result.Content.ReadAsStringAsync();

                    oResultado = JsonConvert.DeserializeObject<RespuestaPostEnvioMultipleFiltro>(contex.Result.ToString());

                    
                    Console.WriteLine("****Transformando Respuesta a DeserializeObject RespuestaPostEnvio ******");

                    /*
                    Console.WriteLine("****transaccionId******" + oResultado.transaccionId);
                    Console.WriteLine("****id******" + oResultado.id);
                    Console.WriteLine("****fechaRegistro******" + oResultado.fechaRegistro);*/
                    Console.WriteLine("****message******" + oResultado.message);
                    xCCupo++;
                }
                //Console.ReadLine();
                //}
                return oResultado;
        }

        public static RespuestaPostEnvioMultipleFiltro fn_obtenerCobranzasLote_Excel(string ResToken)
        {

            string Api_envio = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas/lote/obtenerCobranzas";

            RespuestaPostEnvioMultipleFiltro oResultado = new RespuestaPostEnvioMultipleFiltro();
            string JSON = "";
            string sResultado = "";

            //string sQueryExcel = "select * from [Pendientes_EECC$] where ESTADO LIKE '%PENDIENTE%' ";
            string sQueryExcel = "select * from [Pendientes_EECC$] ";
            
            Console.WriteLine("****sQueryExcel******" + sQueryExcel);
            Console.WriteLine("****sBase******" + sRutaExcel);
            DataTable oObj_EECC = fn_LeerExcel(sRutaExcel, "Pendientes_EECC", sQueryExcel);
            string NRO_CUPON = "";
            string[] arrCupones = new string[1];

            int xCCupo = 0;
            string ID_SOCIO = "";
            string NroFactura_2 = "";
            string sIDOperacion_2 = "";

            DBCGeneric oDbGeneric = new DBCGeneric();

            Console.WriteLine("****Cantidad de registros en el origen ******" + oObj_EECC.Rows.Count);
            //Console.ReadLine();

            fn_CargarTabla(oObj_EECC);

            oDbGeneric = new DBCGeneric();

            oObj_EECC = oDbGeneric.fn_ObtenerResultado("Pa_Pendientes_EECC_Excel_Listar");

            foreach (DataRow oRows in oObj_EECC.Rows)
            {
                xCCupo = 0;
                EnvioPostLote oEnvioPost = new EnvioPostLote();

                //oEnvioPost.canal = "OP";//pcanal; CANAL DE OPERACIONES
                //oEnvioPost.codConvenio = "13003"; //convenio ISOLUTIONS CON ENTEL 13004 PROD 13002 PRODUCCION
                //oEnvioPost.codOperacion = "1020"; // AMBOS
                //oEnvioPost.empresaOrigen = "ENTEL";//AMBOS

                oEnvioPost.canal = "OP";//pcanal; CANAL DE OPERACIONES
                oEnvioPost.codConvenio = "13002"; //convenio ISOLUTIONS CON ENTEL 13004 PROD 13002 PRODUCCION
                oEnvioPost.codOperacion = "1020"; // AMBOS
                oEnvioPost.empresaOrigen = "ENTEL";//AMBOS


                NroFactura_2 = oRows["NRO_FACTURA"].ToString();

                Random rnd = new Random();
                int iIDOperacion = rnd.Next(99999999);     // creates a number between 0 and 51
                sIDOperacion_2 = iIDOperacion.ToString("00000000");

                /*
                foreach (DataRow oRes
                                in fn_ObtenerResultado("Select ( MAX(CORRELATIVO) +1 )ULTIMO from " +
                                sEsquema + "\"CORRELATIVO_EECC\"  WHERE VALOR ='" + DateTime.Now.Year
                                + "'").Rows)
                {
                    sIDOperacion_2 = ((DateTime.Now.Year - 2000)
                        + Convert.ToInt32(oRes["ULTIMO"]).ToString("0000000"));

                    fn_Registrar("UPDATE " + sEsquema + "\"CORRELATIVO_EECC\" SET CORRELATIVO =" +
                        Convert.ToInt32(oRes["ULTIMO"]) + " WHERE VALOR ='" + DateTime.Now.Year + "'");
                    fn_Registrar("INSERT INTO " + sEsquema + "\"EQUIVALENCIA_EECC\" (IDENTIFICADOR,ORIGEN,FECHAREGISTRO) VALUES( " +
                        sIDOperacion_2 + " ,'" + NroFactura_2 + "', '" + DateTime.Now + "' ) ");
                }
                */

                oEnvioPost.idOperacion = sIDOperacion_2;//AMBOS

                //oEnvioPost.idOperacion = "230000015";
                //oEnvioPost.idOperacion = sIDOperacion_2;//oRows["NRO_FACTURA"].ToString(); //"12312";

                NRO_CUPON = oRows["NRO_CUPON"].ToString();
                arrCupones[xCCupo] = NRO_CUPON;
                ID_SOCIO = oRows["ID_SOCIO"].ToString();

                //arrCupones = new string[oObj_EECC.Rows.Count];
                //arrCupones = new string[1];
                oEnvioPost.listadoCodigoCobranza = arrCupones;

                /*
                oEnvioPost.listadoCodigoCobranza = new string[2];
                oEnvioPost.listadoCodigoCobranza[0] = "C403339";
                oEnvioPost.listadoCodigoCobranza[1] = "C403340";
                */
                JSON = JsonConvert.SerializeObject(oEnvioPost);

                Console.WriteLine("****JSON ENVIO ******" + JSON);
                Console.WriteLine("****API REST ENVIO ******" + Api_envio);
                Console.WriteLine("****Enviando Serializacion con TOKEN....******" + Api_envio);

                using (var stringContent = new
                 StringContent(JSON,
             System.Text.Encoding.UTF8, "application/json"))
                using (var client = new HttpClient())
                {

                    client.DefaultRequestHeaders.Authorization = new
                        AuthenticationHeaderValue("Bearer", ResToken);

                    Console.WriteLine("****Obteniendo respuesta POST Async....******" + Api_envio);

                    var response = client.PostAsync(Api_envio, stringContent);

                    //var result = await response.Content.ReadAsStringAsync();
                    var result = response.Result;
                    sResultado = result.ReasonPhrase;
                    var contex = response.Result.Content.ReadAsStringAsync();

                    oResultado = JsonConvert.DeserializeObject<RespuestaPostEnvioMultipleFiltro>(contex.Result.ToString());

                    if (oResultado.data != null)
                    {
                        foreach (var pRes in oResultado.data)
                        {
                            string sEstado = pRes.estado.ToUpper();

                            if (sEstado == "V")
                            {
                                sEstado = "Pendiente";

                            }
                            else if (sEstado == "P")
                            {
                                sEstado = "Cancelado";

                                sQueryExcel = " update [Pendientes_EECC_Excel] set  ESTADO ='" + sEstado + "'  ," +
                                "BANCO ='" + pRes.nombreProveedorPago + "'  ," +
                                "IMPORTE_ABONADO = '" + "S/" + pRes.importe + "'  ," +
                                "NRO_OP = '" + pRes.idOperacion + "'  ," +
                                "FECHA_RPTA = '" + DateTime.Now.ToShortDateString() + "'  ," +
                                "HORA_RPTA = '" + DateTime.Now.ToString("hh:mm:ss") + "'  ," +
                                "F_ABONO = '" + Convert.ToDateTime(pRes.fechaProcesoPago).ToString() + "'  ," + //.ToString("dd-MM-yyyy hh:mm:ss") + "'  " +
                                "CANAL_PAGO = '" + pRes.canal + "'  " + //.ToString("dd-MM-yyyy hh:mm:ss") + "'  " +

                                "where ID ='" + oRows["ID"] + "'";

                                fn_ActualizarSQL(sRutaExcel, sQueryExcel);

                            }
                            else if (sEstado == "E")
                            {
                                sEstado = "Anulado";
                            }


                            Console.WriteLine("****sQueryExcel Update******" + sQueryExcel);
                        }
                    }
                    Console.WriteLine("****Transformando Respuesta a DeserializeObject RespuestaPostEnvio ******");

                    /*
                    Console.WriteLine("****transaccionId******" + oResultado.transaccionId);
                    Console.WriteLine("****id******" + oResultado.id);
                    Console.WriteLine("****fechaRegistro******" + oResultado.fechaRegistro);*/
                    Console.WriteLine("****message******" + oResultado.message);
                    xCCupo++;
                }
                //Console.ReadLine();
            }
            return oResultado;
        }

        public static bool fn_CargarTabla(DataTable oObjPendiente)
        {
            try
            {
            string consStringINTERNET = ConfigurationManager.ConnectionStrings["CnxSql"].ConnectionString;
            int iColum = 0;

            FileInfo fi = new FileInfo(sRutaExcel);
            DateTime oFechaArchivo = fi.LastWriteTime;

            DBCGeneric oDBGeneric = new DBCGeneric();
            bool bExiste = false;

                Console.WriteLine(fi.Name);
                Console.WriteLine(fi.CreationTime);
                Console.WriteLine(fi.LastAccessTime);
                Console.WriteLine(fi.LastWriteTime);
                Console.WriteLine(sRutaExcel);
                //Console.ReadLine();

            oDBGeneric = new DBCGeneric();
            foreach (DataRow row in  oDBGeneric.fn_ObtenerResultado("Pa_Pendiente_EECC_Excel_Archivo_Validar",fi.Name,fi.LastWriteTime).Rows )
            {
                bExiste = true;

            }

                Console.WriteLine("!bExiste " + bExiste);
                //Console.ReadLine();


                if (!bExiste) {

                oDBGeneric = new DBCGeneric();
                oDBGeneric.fn_AdicionarObjeto("PA_Pendiente_EECC_Excel_Archivo_Adicionar", fi.LastWriteTime,
                    fi.CreationTime, fi.Name);


                Console.WriteLine("*** ES UN ARCHIVO NUEVO ELIMINANDO CONTENIDO****" + fi.Name);

                oDBGeneric = new DBCGeneric();
                oDBGeneric.fn_AdicionarObjeto("Pa_Pendientes_EECC_Excel_Eliminar");

                Console.WriteLine("*** REALIZANDO VOLCADO DE INFORMACION    ****" + fi.Name);

                using (SqlConnection con = new SqlConnection(EncriptacionMartin.MetodoEncriptacion.Desencriptar(consStringINTERNET)))
            {
                using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                {
                    sqlBulkCopy.BulkCopyTimeout = 10800;

                    //Set the database table name
                    sqlBulkCopy.DestinationTableName = "dbo." + "Pendientes_EECC_Excel";

                    // oDBGeneric = new MDDBCDataAccess.Maestros.DBCGeneric("Conexion_BDInternet");
                    foreach (DataColumn oColumnasSOLICITUD_IMP in oObjPendiente.Columns)
                    {
                        sqlBulkCopy.ColumnMappings.Add(iColum, iColum+1);
                        iColum++;
                        if (iColum > 19)
                        { 
                            break;
                             }
                    }
                    con.Open();
                    sqlBulkCopy.WriteToServer(oObjPendiente);
                    con.Close();
                }
            }
            }


            }
            catch (Exception ex)
            {
                 Console.WriteLine (ex.ToString());
            }

            return true;
        }

        public static RespuestaPostEnvioMultipleFiltro fn_obtenerCobranzasLote(string ResToken)
        {
            
            string Api_envio = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas/lote/obtenerCobranzas";

            RespuestaPostEnvioMultipleFiltro oResultado = new RespuestaPostEnvioMultipleFiltro();
            string JSON = "";            
            string sResultado = "";            
             
            string sQueryExcel = "select * from [Pendientes_EECC$] where ESTADO LIKE '%PENDIENTE%' ";
            ;
            Console.WriteLine("****sQueryExcel******" + sQueryExcel);
            Console.WriteLine("****sBase******" + sRutaExcel);
            DataTable oObj_EECC = fn_LeerExcel(sRutaExcel, "Pendientes_EECC", sQueryExcel);
            string NRO_CUPON = "";
            string[] arrCupones = new string[1];

            int xCCupo = 0;
            string ID_SOCIO = "";
            string NroFactura_2 = "";
            string sIDOperacion_2 = "";
            foreach (DataRow oRows in oObj_EECC.Rows)
            {
                xCCupo = 0;
                EnvioPostLote oEnvioPost = new EnvioPostLote();

                //oEnvioPost.canal = "OP";//pcanal; CANAL DE OPERACIONES
                //oEnvioPost.codConvenio = "13003"; //convenio ISOLUTIONS CON ENTEL 13004 PROD 13002 PRODUCCION
                //oEnvioPost.codOperacion = "1020"; // AMBOS
                //oEnvioPost.empresaOrigen = "ENTEL";//AMBOS

                oEnvioPost.canal = "OP";//pcanal; CANAL DE OPERACIONES
                oEnvioPost.codConvenio = "13002"; //convenio ISOLUTIONS CON ENTEL 13004 PROD 13002 PRODUCCION
                oEnvioPost.codOperacion = "1020"; // AMBOS
                oEnvioPost.empresaOrigen = "ENTEL";//AMBOS


                NroFactura_2 = oRows["NRO_FACTURA"].ToString();
                
                Random rnd = new Random();
                int iIDOperacion = rnd.Next(99999999);     // creates a number between 0 and 51
                sIDOperacion_2 = iIDOperacion.ToString("00000000");

                /*
                foreach (DataRow oRes
                                in fn_ObtenerResultado("Select ( MAX(CORRELATIVO) +1 )ULTIMO from " +
                                sEsquema + "\"CORRELATIVO_EECC\"  WHERE VALOR ='" + DateTime.Now.Year
                                + "'").Rows)
                {
                    sIDOperacion_2 = ((DateTime.Now.Year - 2000)
                        + Convert.ToInt32(oRes["ULTIMO"]).ToString("0000000"));

                    fn_Registrar("UPDATE " + sEsquema + "\"CORRELATIVO_EECC\" SET CORRELATIVO =" +
                        Convert.ToInt32(oRes["ULTIMO"]) + " WHERE VALOR ='" + DateTime.Now.Year + "'");
                    fn_Registrar("INSERT INTO " + sEsquema + "\"EQUIVALENCIA_EECC\" (IDENTIFICADOR,ORIGEN,FECHAREGISTRO) VALUES( " +
                        sIDOperacion_2 + " ,'" + NroFactura_2 + "', '" + DateTime.Now + "' ) ");
                }
                */

                oEnvioPost.idOperacion = sIDOperacion_2;//AMBOS

                //oEnvioPost.idOperacion = "230000015";
                //oEnvioPost.idOperacion = sIDOperacion_2;//oRows["NRO_FACTURA"].ToString(); //"12312";

                NRO_CUPON = oRows["NRO_CUPON"].ToString();
                arrCupones[xCCupo] = NRO_CUPON;
                ID_SOCIO = oRows["ID_SOCIO"].ToString();

                //arrCupones = new string[oObj_EECC.Rows.Count];
                //arrCupones = new string[1];
                oEnvioPost.listadoCodigoCobranza = arrCupones;

            /*
            oEnvioPost.listadoCodigoCobranza = new string[2];
            oEnvioPost.listadoCodigoCobranza[0] = "C403339";
            oEnvioPost.listadoCodigoCobranza[1] = "C403340";
            */
            JSON = JsonConvert.SerializeObject(oEnvioPost);

            Console.WriteLine("****JSON ENVIO ******" + JSON);
            Console.WriteLine("****API REST ENVIO ******" + Api_envio);
            Console.WriteLine("****Enviando Serializacion con TOKEN....******" + Api_envio);

                using (var stringContent = new
                 StringContent(JSON,
             System.Text.Encoding.UTF8, "application/json"))
                using (var client = new HttpClient())
                {

                    client.DefaultRequestHeaders.Authorization = new
                        AuthenticationHeaderValue("Bearer", ResToken);

                    Console.WriteLine("****Obteniendo respuesta POST Async....******" + Api_envio);

                    var response = client.PostAsync(Api_envio, stringContent);

                    //var result = await response.Content.ReadAsStringAsync();
                    var result = response.Result;
                    sResultado = result.ReasonPhrase;
                    var contex = response.Result.Content.ReadAsStringAsync();

                    oResultado = JsonConvert.DeserializeObject<RespuestaPostEnvioMultipleFiltro>(contex.Result.ToString());

                    if (oResultado.data != null)
                    {
                        foreach (var pRes in oResultado.data)
                        {
                            string sEstado = pRes.estado.ToUpper();

                            if (sEstado == "V")
                            {
                                sEstado = "Pendiente";

                            }
                            else if (sEstado == "P")
                            {
                                sEstado = "Cancelado";

                                sQueryExcel = " update [Pendientes_EECC$] set  ESTADO ='" + sEstado + "'  ," +
                                "BANCO ='" + pRes.nombreProveedorPago + "'  ," +
                                "IMPORTE_ABONADO = '" + "S/" + pRes.importe + "'  ," +
                                "NRO_OP = '" + pRes.idOperacion + "'  ," +
                                "FECHA_RPTA = '" + DateTime.Now.ToShortDateString() + "'  ," +
                                "HORA_RPTA = '" + DateTime.Now.ToString("hh:mm:ss") + "'  ," +
                                "F_ABONO = '" + Convert.ToDateTime(pRes.fechaProcesoPago).ToString() + "'  ," + //.ToString("dd-MM-yyyy hh:mm:ss") + "'  " +
                                "CANAL_PAGO = '" +  pRes.canal  + "'  " + //.ToString("dd-MM-yyyy hh:mm:ss") + "'  " +
                                
                                "where ID_SOCIO ='" + pRes.idCliente + "'";

                                fn_ActualizarExcel(sRutaExcel, sQueryExcel);

                            }
                            else if (sEstado == "E")
                            {
                                sEstado = "Anulado";
                            }
                             
                            
                            Console.WriteLine("****sQueryExcel Update******" + sQueryExcel);
                        }
                    }
                    Console.WriteLine("****Transformando Respuesta a DeserializeObject RespuestaPostEnvio ******");

                    /*
                    Console.WriteLine("****transaccionId******" + oResultado.transaccionId);
                    Console.WriteLine("****id******" + oResultado.id);
                    Console.WriteLine("****fechaRegistro******" + oResultado.fechaRegistro);*/
                    Console.WriteLine("****message******" + oResultado.message);
                    xCCupo++;
                }
                //Console.ReadLine();
            }
            return oResultado;
        }


        static void fn_ApiConsumerGet(string pRestToken)
        {

            RespuestaPostEnvioMultipleFiltro oRespuestaPostEnvioMultipleFiltro = null;
            /*
             
            V: Vigente (Pendiente de pago)
P: Pagado (Cupón pagado)
E: Extornado por el cliente (Cupón Anulado) 

             */
            string Api_envio = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas?estado=P";
            string sResultado = "";
            string JSON = "";
            using (var stringContent = new
             StringContent(JSON,
         System.Text.Encoding.UTF8, "application/json"))
            using (var client = new HttpClient())
            {

                client.DefaultRequestHeaders.Authorization = new
                    AuthenticationHeaderValue("Bearer", pRestToken);

                Console.WriteLine("****Obteniendo respuesta POST Async....******" + Api_envio);

                client.DefaultRequestHeaders
                .Accept
                .Add(new MediaTypeWithQualityHeaderValue("application/json"));//ACCEPT header

                /*
                HttpRequestMessage request = new HttpRequestMessage();
                request.RequestUri = new Uri("Your_get_URI");
                request.Method = HttpMethod.Get;
                request.Headers.Add("api_key", "1234");
                */

                client.DefaultRequestHeaders.Add("canal", "CN");
                client.DefaultRequestHeaders.Add("codconvenio", "13003");
                client.DefaultRequestHeaders.Add("codoperacion", "1010");
                client.DefaultRequestHeaders.Add("empresaorigen", "Entel DEV");
                client.DefaultRequestHeaders.Add("idcliente", "22003");
                client.DefaultRequestHeaders.Add("idoperacion", "9665854");

                var response = client.GetAsync(Api_envio);

                //var result = await response.Content.ReadAsStringAsync();
                var result = response.Result;
                sResultado = result.ReasonPhrase;
                var contex = response.Result.Content.ReadAsStringAsync();

                oRespuestaPostEnvioMultipleFiltro = JsonConvert.DeserializeObject<RespuestaPostEnvioMultipleFiltro>(contex.Result.ToString());

                Console.WriteLine("****Transformando Respuesta a DeserializeObject RespuestaPostEnvio ******");

                Console.WriteLine("****transaccionId******" + oRespuestaPostEnvioMultipleFiltro.transaccionId);
                Console.WriteLine("****fechaConsulta******" + oRespuestaPostEnvioMultipleFiltro.fechaConsulta);
                Console.WriteLine("****message******" + oRespuestaPostEnvioMultipleFiltro.message);

                //Console.ReadLine();
            }

        }


        static void fn_ApiConsumerGetMultipleParametro(string pRestToken)
        {

            RespuestaPostEnvioMultipleFiltro oRespuestaPostEnvioMultipleFiltro = null;
            /*
             
            V: Vigente (Pendiente de pago)
P: Pagado (Cupón pagado)
E: Extornado por el cliente (Cupón Anulado) 

             */
            string Api_envio = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas?estado=P";
            string sResultado = "";
            string JSON = "";
            using (var stringContent = new
             StringContent(JSON,
         System.Text.Encoding.UTF8, "application/json"))
            using (var client = new HttpClient())
            {

                client.DefaultRequestHeaders.Authorization = new
                    AuthenticationHeaderValue("Bearer", pRestToken);

                Console.WriteLine("****Obteniendo respuesta POST Async....******" + Api_envio);
                 
                client.DefaultRequestHeaders
                .Accept
                .Add(new MediaTypeWithQualityHeaderValue("application/json"));//ACCEPT header

                /*
                HttpRequestMessage request = new HttpRequestMessage();
                request.RequestUri = new Uri("Your_get_URI");
                request.Method = HttpMethod.Get;
                request.Headers.Add("api_key", "1234");
                */

                client.DefaultRequestHeaders.Add("canal", "CN");
                client.DefaultRequestHeaders.Add("codconvenio", "13003");
                client.DefaultRequestHeaders.Add("codoperacion", "1010");
                client.DefaultRequestHeaders.Add("empresaorigen", "Entel DEV");
                client.DefaultRequestHeaders.Add("idcliente", "22003");
                client.DefaultRequestHeaders.Add("idoperacion", "9665854");
                 
                var response = client.GetAsync(Api_envio);

                //var result = await response.Content.ReadAsStringAsync();
                var result = response.Result;
                sResultado = result.ReasonPhrase;
                var contex = response.Result.Content.ReadAsStringAsync();

                oRespuestaPostEnvioMultipleFiltro = JsonConvert.DeserializeObject<RespuestaPostEnvioMultipleFiltro>(contex.Result.ToString());

                Console.WriteLine("****Transformando Respuesta a DeserializeObject RespuestaPostEnvio ******");

                Console.WriteLine("****transaccionId******" + oRespuestaPostEnvioMultipleFiltro.transaccionId);
                Console.WriteLine("****fechaConsulta******" + oRespuestaPostEnvioMultipleFiltro.fechaConsulta);
                Console.WriteLine("****message******" + oRespuestaPostEnvioMultipleFiltro.message);
                 
                //Console.ReadLine();
            }
             
    }

        public static void fn_ApiConsumerOracleEECC()
        {

            string Api = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/users/login";
            //string Api_envio = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas";
            string JSON = "";
            //string Token = "";
            string sResultado = "";
            Console.WriteLine("****Conectando API******");
            LoginPost oLogin = new LoginPost();
            oLogin.email = "fasty.entel@insolutions.pe";
            oLogin.password = "3Qv6M8#@w$N97Kqr";
            string ResToken = "";
            JSON = JsonConvert.SerializeObject(oLogin);
            Console.WriteLine("****JSON******" + JSON);

            //JSON = "{'email':'fasty.entel@insolutions.pe','password':'3Qv6M8#@w$N97Kqr'}";
            //JSON = "{'email':'admindev@entel.pe','password':'vBE8!r36DT@sYhFt'}";

            using (var stringContent = new
          StringContent(JSON,
      System.Text.Encoding.UTF8, "application/json"))
            using (var client = new
                HttpClient())
            {

                Console.WriteLine("****API REST******");
                var response = client.PostAsync(Api, stringContent);
                var result = response.Result;
                sResultado = result.ReasonPhrase;
                var contex = response.Result.Content.ReadAsStringAsync();
                ResultadoPost oResultado = JsonConvert.DeserializeObject<ResultadoPost>(contex.Result.ToString());
                ResToken = oResultado.token;

                Console.WriteLine("****TOKEN******" + ResToken);

            }

            fn_obtenerCobranzasLote_OracleEECC(ResToken);


        }

        public static void fn_ApiConsumerOracle()
        {
             
            string Api = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/users/login";
            //string Api_envio = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas";
            string JSON = "";
            //string Token = "";
            string sResultado = "";
            Console.WriteLine("****Conectando API******");
            LoginPost oLogin = new LoginPost();
            oLogin.email = "fasty.entel@insolutions.pe";
            oLogin.password = "3Qv6M8#@w$N97Kqr";
            string ResToken = "";
            JSON = JsonConvert.SerializeObject(oLogin);
            Console.WriteLine("****JSON******" + JSON);

            //JSON = "{'email':'fasty.entel@insolutions.pe','password':'3Qv6M8#@w$N97Kqr'}";
            //JSON = "{'email':'admindev@entel.pe','password':'vBE8!r36DT@sYhFt'}";

            using (var stringContent = new
          StringContent(JSON,
      System.Text.Encoding.UTF8, "application/json"))
            using (var client = new
                HttpClient())
            {

                Console.WriteLine("****API REST******");
                var response = client.PostAsync(Api, stringContent);
                var result = response.Result;
                sResultado = result.ReasonPhrase;
                var contex = response.Result.Content.ReadAsStringAsync();
                ResultadoPost oResultado = JsonConvert.DeserializeObject<ResultadoPost>(contex.Result.ToString());
                ResToken = oResultado.token;

                Console.WriteLine("****TOKEN******" + ResToken);
                
               }

                fn_obtenerCobranzasLote_Oracle(ResToken);
                
            
        }

        public RespuestaPostEnvio fn_EnviarComprobante(
                 string Api_envio, string ResToken, string pcanal, string pfechaVencimiento, string pImportePendiente,
                 string pidCliente, string pSocio, string pidOperacion, string codConvenio)
        {
            RespuestaPostEnvio oResultado = new RespuestaPostEnvio();
            string JSON = "";
            //JSON = "{""canal"":""OP"",""codConvenio"":13003,""codOperacion"":2010,""empresaOrigen"":""Entel"",""fechaVencimiento"":""2021-05-30"",""idCliente"":""TESTDESARROLLO"",""idOperacion"":2022,""importe"":1,""moneda"":""PEN"",""nombreCliente"":""Contoso""}";
            string sResultado = "";
            EnvioPost oEnvioPost = new EnvioPost();
            oEnvioPost.canal = pcanal;//pcanal; CANAL DE OPERACIONES
            //oEnvioPost.codConvenio = "13003"; //convenio ISOLUTIONS CON ENTEL 13004 PROD
            oEnvioPost.codConvenio = codConvenio;
            oEnvioPost.codOperacion = "2010"; // AMBOS
            oEnvioPost.empresaOrigen = "Entel";//AMBOS
            string[] sFecha = pfechaVencimiento.Split(@"/");
            //string[] sFecha = new string[2];
            oEnvioPost.fechaVencimiento = (sFecha[2] + "-" + sFecha[1] + "-" + sFecha[0]);
            oEnvioPost.idCliente = pidCliente;
            oEnvioPost.idOperacion = pidOperacion.Replace("-", "");
            oEnvioPost.importe = Convert.ToDecimal(pImportePendiente.Replace(",", "")).ToString();
            oEnvioPost.moneda = "PEN";
            oEnvioPost.nombreCliente = pSocio;

            JSON = JsonConvert.SerializeObject(oEnvioPost);

            Console.WriteLine("****JSON ENVIO ******" + JSON);
            Console.WriteLine("****API REST ENVIO ******" + Api_envio);
            Console.WriteLine("****Enviando Serializacion con TOKEN....******" + Api_envio);

            using (var stringContent = new
             StringContent(JSON,
         System.Text.Encoding.UTF8, "application/json"))
            using (var client = new HttpClient())
            {
                client.DefaultRequestHeaders.Authorization = new
                    AuthenticationHeaderValue("Bearer", ResToken);

                Console.WriteLine("****Obteniendo respuesta POST Async....******" + Api_envio);

                var response = client.PostAsync(Api_envio, stringContent);

                //var result = await response.Content.ReadAsStringAsync();
                var result = response.Result;
                sResultado = result.ReasonPhrase;
                var contex = response.Result.Content.ReadAsStringAsync();

                oResultado = JsonConvert.DeserializeObject<RespuestaPostEnvio>(contex.Result.ToString());

                Console.WriteLine("****Transformando Respuesta a DeserializeObject RespuestaPostEnvio ******");
                Console.WriteLine("****transaccionId******" + oResultado.transaccionId);
                Console.WriteLine("****id******" + oResultado.id);
                Console.WriteLine("****fechaRegistro******" + oResultado.fechaRegistro);
                Console.WriteLine("****message******" + oResultado.message);
                //Console.ReadLine();
            }

            //oResultado.transaccionId = "C410"+ DateTime.Now.ToShortDateString(); ;
            return oResultado;
        }

        public static void fn_ActualizarEstadoCupon_Excel_Produccion_new()
        {
            string ApiLogin = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/users/login";
            //string Api = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/users/login";
            //string Api_envio = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas";
            //string urlid =     "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas";
            string JSON = "";
            string Token = "";
            string sResultado = "";
            Console.WriteLine("****Conectando API******");
            LoginPost oLogin = new LoginPost();
            oLogin.email = "fasty.entel@insolutions.pe";
            oLogin.password = "3Qv6M8#@w$N97Kqr";
            string ResToken = "";
            JSON = JsonConvert.SerializeObject(oLogin);
            Console.WriteLine("****JSON******" + JSON);

            //JSON = "{'email':'fasty.entel@insolutions.pe','password':'3Qv6M8#@w$N97Kqr'}";
            //JSON = "{'email':'admindev@entel.pe','password':'vBE8!r36DT@sYhFt'}";

            using (var stringContent = new
          StringContent(JSON,
      System.Text.Encoding.UTF8, "application/json"))
            using (var client = new HttpClient())
            {

                Console.WriteLine("****API REST******");
                var response = client.PostAsync(ApiLogin, stringContent);
                var result = response.Result;
                sResultado = result.ReasonPhrase;
                var contex = response.Result.Content.ReadAsStringAsync();
                ResultadoPost oResultado = JsonConvert.DeserializeObject<ResultadoPost>(contex.Result.ToString());
                ResToken = oResultado.token;

                Console.WriteLine("****TOKEN******" + ResToken);
                //Cadena = Token;
                //Console.ReadLine();

            }


            fn_obtenerCobranzasLote_Excel(ResToken);


        }

        public static void fn_ActualizarEstadoCupon_Excel_Produccion()
        {
            string ApiLogin = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/users/login";
            //string Api = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/users/login";
            //string Api_envio = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas";
            //string urlid =     "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas";
            string JSON = "";
            string Token = "";
            string sResultado = "";
            Console.WriteLine("****Conectando API******");
            LoginPost oLogin = new LoginPost();
            oLogin.email = "fasty.entel@insolutions.pe";
            oLogin.password = "3Qv6M8#@w$N97Kqr";
            string ResToken = "";
            JSON = JsonConvert.SerializeObject(oLogin);
            Console.WriteLine("****JSON******" + JSON);

            //JSON = "{'email':'fasty.entel@insolutions.pe','password':'3Qv6M8#@w$N97Kqr'}";
            //JSON = "{'email':'admindev@entel.pe','password':'vBE8!r36DT@sYhFt'}";

            using (var stringContent = new
          StringContent(JSON,
      System.Text.Encoding.UTF8, "application/json"))
            using (var client = new HttpClient())
            {

                Console.WriteLine("****API REST******");
                var response = client.PostAsync(ApiLogin, stringContent);
                var result = response.Result;
                sResultado = result.ReasonPhrase;
                var contex = response.Result.Content.ReadAsStringAsync();
                ResultadoPost oResultado = JsonConvert.DeserializeObject<ResultadoPost>(contex.Result.ToString());
                ResToken = oResultado.token;

                Console.WriteLine("****TOKEN******" + ResToken);
                //Cadena = Token;
                //Console.ReadLine();

            }

             
            fn_obtenerCobranzasLote(ResToken);


             }

            public static void fn_ApiConsumerEstadoCuponExcel()
        {
            //string Api = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/users/login";
            string Api = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/users/login";
            string Api_envio = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas";
            //string urlid =     "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas";
            string JSON = "";
            string Token = "";
            string sResultado = "";
            Console.WriteLine("****Conectando API******");
            LoginPost oLogin = new LoginPost();
            oLogin.email = "admindev@entel.pe";
            oLogin.password = "vBE8!r36DT@sYhFt";
            string ResToken = "";
            JSON = JsonConvert.SerializeObject(oLogin);
            Console.WriteLine("****JSON******" + JSON);

            //JSON = "{'email':'fasty.entel@insolutions.pe','password':'3Qv6M8#@w$N97Kqr'}";
            //JSON = "{'email':'admindev@entel.pe','password':'vBE8!r36DT@sYhFt'}";

            using (var stringContent = new
          StringContent(JSON,
      System.Text.Encoding.UTF8, "application/json"))
            using (var client = new HttpClient())
            {

                Console.WriteLine("****API REST******");
                var response = client.PostAsync(Api, stringContent);
                var result = response.Result;
                sResultado = result.ReasonPhrase;
                var contex = response.Result.Content.ReadAsStringAsync();
                ResultadoPost oResultado = JsonConvert.DeserializeObject<ResultadoPost>(contex.Result.ToString());
                ResToken = oResultado.token;

                Console.WriteLine("****TOKEN******" + ResToken);
                //Cadena = Token;
                //Console.ReadLine();

            }



            fn_obtenerCobranzasLote(ResToken);
      
                 
             
        }



        public static DataTable fn_LeerExcel(string pRutaBaseArchivo,string pHoja,string sQueryExcel)
        {
            DataTable oObj = new DataTable();
            string sBase = pRutaBaseArchivo; //System.Configuration.ConfigurationManager.AppSettings["param1"] ;
            string sHoja = pHoja;//System.Configuration.ConfigurationManager.AppSettings["param2"] ;

            //string sBase =  System.Configuration.ConfigurationManager.AppSettings["param1"] ;
            //string sHoja = System.Configuration.ConfigurationManager.AppSettings["param2"] ;

            int xValor = 1;
            //public static string connStr = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + path + ";Extended Properties=excel 12.0;";

            //Fuente: https://www.iteramos.com/pregunta/9358/excel-quotla-tabla-externa-no-tiene-el-formato-esperadoquot
            System.Data.DataSet DtSet;
            DtSet = new System.Data.DataSet();

            Console.WriteLine("********Conectandose a archivo Excel************************" + pRutaBaseArchivo);

            Console.WriteLine("********Inicio ********" + DateTime.Now);

            Console.WriteLine("********sBase********" + sBase);
            
            //Console.Read();

            //using (System.Data.OleDb.OleDbConnection oOleDbConnection = new System.Data.OleDb.OleDbConnection("Provider=Microsoft.ACE.OLEDB.12.0;Data Source='" + sBase + "';Extended Properties=excel 12.0;"))
            using (System.Data.OleDb.OleDbConnection oOleDbConnection = new System.Data.OleDb.OleDbConnection("Provider = Microsoft.ACE.OLEDB.12.0; Data Source='" + sBase + "';Extended Properties = \"Excel 12.0 Xml;  HDR=YES; IMEX=1\";"))
            { 
                    oOleDbConnection.Open();
                    using (System.Data.OleDb.OleDbDataAdapter oOleDbDataAdapterTotal =
            new System.Data.OleDb.OleDbDataAdapter(sQueryExcel, oOleDbConnection))
                    {

                        Console.WriteLine("Obteniendo registros de la hoja  " + sHoja + "............");
                        //  Console.Read();

                        oOleDbDataAdapterTotal.TableMappings.Add("Table", "TestTable");
                        DtSet = new System.Data.DataSet();
                        oOleDbDataAdapterTotal.Fill(DtSet);

                        oObj = DtSet.Tables[0];

                     
                }

            }


            return oObj;
        }

        public static RespuestaPostEnvio fn_ApiPatch_Vencimiento(
            string pmail, string ppassword,
                    string pAPI_LOGIN,

                    string pAPI_PATCH,
                    string pcanal, string pcodOperacion,
                    string pcodConvenio, string pidTrazabilidad, string pcodigoCobranzaAsociada,
                    string pfechaVencimiento

            )
        {
            RespuestaPostEnvio oResultado = null;

            string accessToken = "";
            //string Api = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas/C403635";
            EnvioPost4 oEnvioPost = new EnvioPost4();
            //oEnvioPost.canal = "OP";//pcanal; CANAL DE OPERACIONES
            //oEnvioPost.codConvenio = "13003"; //convenio ISOLUTIONS CON ENTEL 13004 PROD
            //oEnvioPost.codConvenio = "13003";
            //oEnvioPost.codOperacion = "3010"; // AMBOS
            //oEnvioPost.empresaOrigen = "Entel";//AMBOS
            //oEnvioPost.idOperacion = "230000084" ;

            oEnvioPost.canal = pcanal;//pcanal; CANAL DE OPERACIONES
            oEnvioPost.codOperacion = "2010"; //convenio ISOLUTIONS CON ENTEL 13004 PROD
            oEnvioPost.codConvenio = pcodConvenio; // AMBOS
            oEnvioPost.idTrazabilidad = pidTrazabilidad;//AMBOS
            oEnvioPost.codigoCobranzaAsociada = pcodigoCobranzaAsociada;

            string[] sFecha = pfechaVencimiento.Split(@"/");
            //string[] sFecha = new string[2];
            oEnvioPost.fechaVencimiento = (sFecha[2] + "-" + sFecha[1] + "-" + sFecha[0]);

            //oEnvioPost.fechaVencimiento = pfechaVencimiento;

            string JSON = JsonConvert.SerializeObject(oEnvioPost);

            //throw new Exception(JSON);

            HttpClient oHttpClient = new HttpClient();
            var request = new HttpRequestMessage(new HttpMethod("PATCH"), pAPI_PATCH)
            {
                Content = new StringContent(JSON,
                                    Encoding.UTF8,
                                    "application/json")//CONTENT-TYPE header
            };
            oHttpClient.DefaultRequestHeaders.Authorization =
    new AuthenticationHeaderValue("Bearer", fn_ObtenerToken_2(
        //"admindev@entel.pe", "vBE8!r36DT@sYhFt", "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/users/login"));
        pmail, ppassword, pAPI_LOGIN));
            oHttpClient.DefaultRequestHeaders.ExpectContinue = false;
            var response = oHttpClient.SendAsync(request);

            //if (!response.Result.IsSuccessStatusCode)
            //{
            var responseCode = response.Result.StatusCode;
            var responseJson = response.Result.Content.ReadAsStringAsync();
             
            oResultado = JsonConvert.DeserializeObject<RespuestaPostEnvio>(responseJson.Result);
             
            return oResultado;

        }

        public static string fn_GenerarFechaVencimiento_main()
        {
            string sValor = "";
            RespuestaPostEnvio oRespuestaPostEnvio = null;
            DBCGeneric oDbGeneric = new DBCGeneric();
            string Api_envio = "", ResToken = "", pcanal = "", pfechaVencimiento = "", pImportePendiente = "",
                     pidCliente = "", pSocio = "", pidOperacion = "", codConvenio = "";
            string email = "", password = "", Api = "";
            string pApi_Patch = "";
            string sPRODDEVE = "PROD";
            string Api_envio_consulta_cupon = "";
            string codConvenio_2 = ""; // ENTEL 13004 PROD 13002 PRODUCCION
            string Pa_Pendientes_EECC_Excel_Listar_NF = "";
            string sStoreProcedure_2 = "";
            try
            {
                if (sPRODDEVE == "DEV")
                {
                    email = "admindev@entel.pe";
                    password = "vBE8!r36DT@sYhFt";
                    Api = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/users/login";

                    codConvenio = "13003";
                    Api_envio = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas";
                    pApi_Patch = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas/actualizacion/fecha-vencimiento";
                    Api_envio_consulta_cupon = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas/lote/obtenerCobranzas";
                    codConvenio_2 = "";
                    Pa_Pendientes_EECC_Excel_Listar_NF = "Pa_Pendientes_EECC_Excel_Listar_NF_P";
                    sStoreProcedure_2 = "Pa_CuponExcel_ActualizarEstado_P";
                }
                else if (sPRODDEVE == "PROD")
                {
                    email = "fasty.entel@insolutions.pe";
                    password = "3Qv6M8#@w$N97Kqr";
                    Api = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/users/login";

                    codConvenio = "13004";
                    Api_envio = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas";
                    pApi_Patch = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas/actualizacion/fecha-vencimiento";
                    Api_envio_consulta_cupon= "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas/lote/obtenerCobranzas";
                    codConvenio_2 = "";
                    Pa_Pendientes_EECC_Excel_Listar_NF = "Pa_Pendientes_EECC_Excel_Listar_NF";
                    sStoreProcedure_2 = "Pa_CuponExcel_ActualizarEstado";
                }


                oDbGeneric = new DBCGeneric();

                pcanal = "OP";

                foreach (DataRow oRows in oDbGeneric.fn_ObtenerResultado(Pa_Pendientes_EECC_Excel_Listar_NF).Rows)
                {
                    string pNumeroCupon = oRows["NRO_CUPON"].ToString();
                    string[] IMPORTE_PEND_A = StringExtension.Split(oRows["IMPORTE_PEND"].ToString(), "S/. ");
                    string IMPORTE_PEND = IMPORTE_PEND_A[1];
                    decimal dSaldo = 0;
                    ResToken = fn_ObtenerToken_2(email, password, Api);
                   
                    pfechaVencimiento = oRows["F_VCTO"].ToString();
                        pImportePendiente = IMPORTE_PEND;
                        pidCliente = oRows["ID_SOCIO"].ToString();
                        pSocio = oRows["SOCIO"].ToString();
                        Random rnd = new Random();
                        int iIDOperacion = rnd.Next(99999999);     // creates a number between 0 and 51
                        pidOperacion = iIDOperacion.ToString("00000000");

                        //codConvenio = "";
                        string sCodigoCanal = "";
                        /* 86 = OP - 87 =RM */
                        string sIDCANAL = "86";
                        sCodigoCanal = "OP";

                    /*foreach (DataRow oRows_R in fn_ObtenerResultado("SELECT * FROM TABLA_P WHERE PKID ='" + sIDCANAL + "'").Rows)
                        {
                            codConvenio = oRows["ABREVIATURA"].ToString();
                            sCodigoCanal = oRows["CODIGO"].ToString();
                        
                    }
                    */

                    /*
                        email = "admindev@entel.pe";
                    password = "vBE8!r36DT@sYhFt";
                    Api = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/users/login";

                    codConvenio = "13003";
                     */
                    string email_token = email;
                        string clave_token = password;
                        string Api_token = Api;//"https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/users/login";
                        string canal = sCodigoCanal;
                        //codConvenio = codConvenio; //"13004"; //13004
                        string Api_Patch = pApi_Patch;
                        string codOperacion = "2010";

                        string importe = pImportePendiente;
                        string referencia = "NC-ND";
                        string idOperacion = pidOperacion;
                        string numeroCupon = pNumeroCupon;
                        string moneda = "PEN";
                     

                RespuestaPostEnvio oRespuestaPostEnvio2= fn_ApiPatch_Vencimiento(email, password, Api_token, pApi_Patch, canal, 
                        idOperacion, codConvenio, idOperacion, numeroCupon, pfechaVencimiento);

                    if (oRespuestaPostEnvio2 != null) {

                        if(oRespuestaPostEnvio2.message.Contains("La fecha de vencimiento de la cobranza se actualizó con éxito")) {  
                        RespuestaPostEnvioMultipleFiltro oRespuestaPostEnvioMultipleFiltro = fn_obtenerCobranzasLote_Excel_Unitario(ResToken, numeroCupon,Api_envio_consulta_cupon, codConvenio);

                        if(oRespuestaPostEnvioMultipleFiltro.data!=null)
                        {
                            foreach(var oFiltro in oRespuestaPostEnvioMultipleFiltro.data)
                            {
                                 if( Convert.ToDateTime( oFiltro.fechaVencimiento)== Convert.ToDateTime( pfechaVencimiento))
                                {
                                    oDbGeneric = new DBCGeneric();
                                    oDbGeneric.fn_AdicionarObjeto(sStoreProcedure_2, oRows["ID"]);
                                }

                            }

                        }
                       }                         
                    }
                }
            }
            catch (Exception ex)
            {

            }

            return sValor;
        }

        public static string fn_GenerarCupoNC()
        {
            string sValor = "";
            RespuestaPostEnvio oRespuestaPostEnvio = null;
            DBCGeneric oDbGeneric = new DBCGeneric();
            string Api_envio = "", ResToken = "", pcanal = "", pfechaVencimiento = "", pImportePendiente = "",
                     pidCliente = "", pSocio = "", pidOperacion = "", codConvenio = "";
            string email = "", password = "", Api = "";
            string pApi_Patch = "";
            string sPRODDEVE = "PROD";
            string sStoreProcedure = "";
            string sStoreProcedure_2 = "";
            string pApi_Patch_NC = "";
            string pApi_Patch_ND = "";
            string Api_envio_consultaCupon = "";
            decimal dSaldo = 0;
            string Pa_CuponExcel_ActualizarImporte = "";
            try
            {
                if (sPRODDEVE == "DEV")
                {
                    email = "admindev@entel.pe";
                    password = "vBE8!r36DT@sYhFt";
                    Api = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/users/login";

                    codConvenio = "13003";
                    Api_envio =  "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas";
                    pApi_Patch = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api//notas/creditos";

                    pApi_Patch_NC = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api//notas/creditos";
                    pApi_Patch_ND = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api//notas/debitos";
                    sStoreProcedure = "Pa_Pendientes_EECC_Excel_Listar_NC_P";
                    sStoreProcedure_2 = "Pa_CuponExcel_ActualizarEstado_P";
                    Api_envio_consultaCupon = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas/lote/obtenerCobranzas";
                    Pa_CuponExcel_ActualizarImporte = "Pa_CuponExcel_ActualizarImporte_P";
                }
                else if (sPRODDEVE == "PROD")
                {
                    email = "fasty.entel@insolutions.pe";
                    password = "3Qv6M8#@w$N97Kqr";
                    Api = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/users/login";

                    codConvenio = "13004";
                    Api_envio = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas";
                    pApi_Patch = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api//notas/creditos";
                    pApi_Patch_NC = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api//notas/creditos";
                    pApi_Patch_ND = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api//notas/debitos";
                    sStoreProcedure = "Pa_Pendientes_EECC_Excel_Listar_NC";
                    sStoreProcedure_2 = "Pa_CuponExcel_ActualizarEstado";
                    Api_envio_consultaCupon = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas/lote/obtenerCobranzas";
                    Pa_CuponExcel_ActualizarImporte = "Pa_CuponExcel_ActualizarImporte";
                }

                oDbGeneric = new DBCGeneric();
                pcanal = "OP";

                foreach (DataRow oRows in oDbGeneric.fn_ObtenerResultado(sStoreProcedure).Rows)
                {
                    string pNumeroCupon = oRows["NRO_CUPON"].ToString();
                    string[] IMPORTE_PEND_A = StringExtension.Split(oRows["IMPORTE_PEND"].ToString(), "S/. ");
                    string IMPORTE_PEND = IMPORTE_PEND_A[1];
                     dSaldo = 0;
                    
                    ResToken = fn_ObtenerToken_2(email, password, Api);

                    RespuestaPostEnvioMultipleFiltro oRespuestaPostEnvioMultipleFiltro= 
                        fn_obtenerCobranzasLote_Excel_Unitario(ResToken, pNumeroCupon,Api_envio_consultaCupon, codConvenio);

                    if (oRespuestaPostEnvioMultipleFiltro != null) {

                        if (oRespuestaPostEnvioMultipleFiltro.data != null) {
                            foreach (var pResulado in oRespuestaPostEnvioMultipleFiltro.data)
                            {
                                dSaldo = Convert.ToDecimal(IMPORTE_PEND) - Convert.ToDecimal(pResulado.importe);
                                decimal NewImporte = 0;


                             
                                if (dSaldo > 0)
                                {
                                    //pApi_Patch = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api//notas/creditos";
                                    pApi_Patch = pApi_Patch_ND;
                                    /*NewImporte = dSaldo + Convert.ToDecimal(IMPORTE_PEND);
                                    oDbGeneric = new DBCGeneric();
                                    oDbGeneric.fn_AdicionarObjeto(Pa_CuponExcel_ActualizarImporte, oRows["ID"], NewImporte);
                                    */
                                }
                                else if (dSaldo < 0)
                                {
                                    //pApi_Patch = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api//notas/debitos";
                                    pApi_Patch = pApi_Patch_NC;
                                    /*NewImporte = dSaldo + Convert.ToDecimal(IMPORTE_PEND);
                                    oDbGeneric = new DBCGeneric();
                                    oDbGeneric.fn_AdicionarObjeto(Pa_CuponExcel_ActualizarImporte, oRows["ID"], NewImporte);
                                    */
                                }
                                else if (dSaldo == 0)
                                {
                                    oDbGeneric = new DBCGeneric();
                                    oDbGeneric.fn_AdicionarObjeto(sStoreProcedure_2, oRows["ID"]);
                                }
                                else
                                {
                                }
                            }
                        }

                        pfechaVencimiento = oRows["F_VCTO"].ToString();
                        pImportePendiente =  dSaldo.ToString() ;
                        pidCliente = oRows["ID_SOCIO"].ToString();
                        pSocio = oRows["SOCIO"].ToString();
                        Random rnd = new Random();
                        int iIDOperacion = rnd.Next(99999999);     // creates a number between 0 and 51
                        pidOperacion = iIDOperacion.ToString("00000000");

                        string sCodigoCanal = "";

                        sCodigoCanal = "OP";

                        //codConvenio = "";
                        /* 86 = OP - 87 =RM */
                        string sIDCANAL = "86";
                        /*
                        foreach (DataRow oRows_R in fn_ObtenerResultado("SELECT * FROM TABLA_P WHERE PKID ='" + sIDCANAL + "'").Rows)
                        {
                            codConvenio = oRows["ABREVIATURA"].ToString();
                            sCodigoCanal = oRows["CODIGO"].ToString();
                        }
                        */

                        //codConvenio = codConvenio;
                        

                        /*
                            email = "admindev@entel.pe";
                        password = "vBE8!r36DT@sYhFt";
                        Api = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/users/login";

                        codConvenio = "13003";
                         */
                        string email_token = email;
                        string clave_token = password;
                        string Api_token = Api; //"https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/users/login";
                        string canal = sCodigoCanal;
                        //codConvenio = codConvenio; //"13004"; //13004
                        string Api_Patch = pApi_Patch;
                        string codOperacion = "2010";

                        string importe = Math.Abs( Convert.ToDecimal( pImportePendiente)).ToString();
                        string referencia = "NC-ND";
                        string idOperacion = pidOperacion;
                        string numeroCupon = pNumeroCupon;
                        string moneda = "PEN";

                      

                        NotaCreditoOutput oNotaCreditoOutput = fn_GenerarNotaCredito_Back(email_token, clave_token, Api_token,
                        canal,
                        codConvenio,
                        Api_Patch, codOperacion, idOperacion, numeroCupon, importe,
                        moneda, referencia);

                        if (oNotaCreditoOutput != null)
                        {
                            //ResToken = fn_ObtenerToken_2(email, password, Api);
                        
                            RespuestaPostEnvioMultipleFiltro oRespuestaPostEnvioMultipleFiltro_Saldo = 
                                fn_obtenerCobranzasLote_Excel_Unitario(ResToken, pNumeroCupon,Api_envio_consultaCupon, codConvenio);

                            if (oRespuestaPostEnvioMultipleFiltro_Saldo.data != null)
                            {
                                foreach (var pResulado in oRespuestaPostEnvioMultipleFiltro_Saldo.data)
                                {

                                    dSaldo = Convert.ToDecimal(IMPORTE_PEND) - Convert.ToDecimal(pResulado.importe);

                                    if (dSaldo > 0)
                                    {
                                        //pApi_Patch = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api//notas/creditos";
                                    }
                                    else if (dSaldo < 0)
                                    {
                                        //pApi_Patch = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api//notas/debitos";
                                    }
                                    else if (dSaldo == 0)
                                    {
                                        oDbGeneric = new DBCGeneric();
                                        oDbGeneric.fn_AdicionarObjeto(sStoreProcedure_2, oRows["ID"]);
                                    }
                                    else
                                    {

                                    }

                                }
                            }

                        }

                        /*
                         if (oNotaCreditoOutput != null)
                         {
                             oDbGeneric = new DBCGeneric();
                             oDbGeneric.fn_AdicionarObjeto("Pa_CuponExcel_Actualizar", oRows["ID"], oRespuestaPostEnvio.id);
                         }
                        */
                    }
                    }
                }
            catch (Exception ex)
            {

            }

            return sValor;
        }

        public static NotaCreditoOutput fn_GenerarNotaCredito_Back(
      string pmail, string ppassword,
              string pAPI_LOGIN,

      string pcanal, string codConvenio,
              string pAPI_POST, string pcodOperacion, string pidTrazabilidad,
              string pcodigoCobranzaAsociada, string pmonto, string pmoneda, string preferencia
      )
        {
            NotaCreditoOutput oResultado = null;

            string accessToken = "";
            //string Api = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas/C403635";
            EnvioPost3 oEnvioPost = new EnvioPost3();
            //oEnvioPost.canal = "OP";//pcanal; CANAL DE OPERACIONES
            //oEnvioPost.codConvenio = "13003"; //convenio ISOLUTIONS CON ENTEL 13004 PROD
            //oEnvioPost.codConvenio = "13003";
            //oEnvioPost.codOperacion = "3010"; // AMBOS
            //oEnvioPost.empresaOrigen = "Entel";//AMBOS
            //oEnvioPost.idOperacion = "230000084" ;

            oEnvioPost.canal = pcanal;//pcanal; CANAL DE OPERACIONES
            oEnvioPost.codOperacion = pcodOperacion; // AMBOS
            oEnvioPost.codConvenio = codConvenio; //convenio ISOLUTIONS CON ENTEL 13004 PROD
            oEnvioPost.idTrazabilidad = pidTrazabilidad;
            oEnvioPost.codigoCobranzaAsociada = pcodigoCobranzaAsociada;
            oEnvioPost.monto = pmonto;
            oEnvioPost.moneda = pmoneda;
            oEnvioPost.referencia = preferencia;

            //oEnvioPost.codConvenio = pempresaOrigen;//AMBOS
            //oEnvioPost.idOperacion = pidOperacion;

            string JSON = JsonConvert.SerializeObject(oEnvioPost);

            //    oHttpClient.DefaultRequestHeaders.Authorization =new AuthenticationHeaderValue("Bearer", fn_ApiConsumer(
            //oResultado = JsonConvert.DeserializeObject<RespuestaPostEnvio>(responseJson.Result);

            string sResultado = "";

            using (var stringContent = new
       StringContent(JSON,
   System.Text.Encoding.UTF8, "application/json"))
            using (var client = new HttpClient())
            {

                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", fn_ObtenerToken_2(pmail, ppassword, pAPI_LOGIN));
                Console.WriteLine("****API REST******");
                var response = client.PostAsync(pAPI_POST, stringContent);
                var result = response.Result;
                sResultado = result.ReasonPhrase;
                var contex = response.Result.Content.ReadAsStringAsync();
                oResultado = JsonConvert.DeserializeObject<NotaCreditoOutput>(contex.Result.ToString());
                //ResToken = oResultado.token;

                //Console.WriteLine("****TOKEN******" + ResToken);
                //Cadena = Token;
                //Console.ReadLine();

            }



            return oResultado;

        }
        public static string fn_GenerarCuponPendientes()
        {
            string sValor = "";
            RespuestaPostEnvio oRespuestaPostEnvio = null;
            DBCGeneric oDbGeneric = new DBCGeneric();
            string Api_envio="",  ResToken = "",  pcanal = "",  pfechaVencimiento = "",  pImportePendiente = "",
                     pidCliente = "",  pSocio = "",  pidOperacion = "",  codConvenio = "";
            string email="", password="", Api = "";

            string sPRODDEVE = "PROD";
            string sStoreProcedure = "";
            string sStoreProcedure_Ac = "";
            try
            {
                if (sPRODDEVE == "DEV")
                {
                    email = "admindev@entel.pe";
                    password = "vBE8!r36DT@sYhFt";
                    Api = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/users/login";

                    codConvenio = "13003";
                    Api_envio = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas";
                    sStoreProcedure = "Pa_Pendientes_EECC_Excel_Listar_Cupon_P";
                    sStoreProcedure_Ac = "Pa_CuponExcel_Actualizar_P";

                }
                else if (sPRODDEVE == "PROD")
                {
                    email = "fasty.entel@insolutions.pe";
                    password = "3Qv6M8#@w$N97Kqr";
                    Api = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/users/login";

                    codConvenio = "13004";
                    Api_envio = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas";
                    sStoreProcedure = "Pa_Pendientes_EECC_Excel_Listar_Cupon";
                    sStoreProcedure_Ac = "Pa_CuponExcel_Actualizar";
                }


                oDbGeneric = new DBCGeneric();

                pcanal = "OP";

                foreach (DataRow oRows in oDbGeneric.fn_ObtenerResultado(sStoreProcedure).Rows)
                {

                    string[] IMPORTE_PEND_A = StringExtension.Split(oRows["IMPORTE_PEND"].ToString(), "S/. ");

                    string IMPORTE_PEND = IMPORTE_PEND_A[1];


                    ResToken = fn_ObtenerToken_2(email, password, Api);

                    pfechaVencimiento = oRows["F_VCTO"].ToString();
                    pImportePendiente = IMPORTE_PEND;
                    pidCliente = oRows["ID_SOCIO"].ToString();
                    pSocio = oRows["SOCIO"].ToString();
                    Random rnd = new Random();
                    int iIDOperacion = rnd.Next(99999999);     // creates a number between 0 and 51
                    pidOperacion = iIDOperacion.ToString("00000000");

                    oRespuestaPostEnvio = fn_GenerarCupon(Api_envio, ResToken, 
                        pcanal, pfechaVencimiento, 
                        pImportePendiente, pidCliente, 
                        pSocio, pidOperacion, codConvenio);

                    if(oRespuestaPostEnvio!=null)
                    {
                        oDbGeneric = new DBCGeneric();
                        oDbGeneric.fn_AdicionarObjeto(sStoreProcedure_Ac, oRows["ID"], oRespuestaPostEnvio.id);                         
                    }
                }
            }
            catch (Exception ex)
            {
                 
            }
            
            return sValor;
        }

        public static string fn_ObtenerToken_2(string email, string password, string Api)
        {
            //string Api = "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/users/login";
            //string Api = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/users/login";
            //string Api_envio = "https://us-central1-api-cliente-cobranzas-dev.cloudfunctions.net/app/api/cobranzas";
            //string urlid =     "https://us-central1-api-fasty-produccion.cloudfunctions.net/app/api/cobranzas";
            string JSON = "";
            string Token = "";
            string sResultado = "";
            Console.WriteLine("****Conectando API******");
            LoginPost oLogin = new LoginPost();
            //oLogin.email = "admindev@entel.pe";
            //oLogin.password = "vBE8!r36DT@sYhFt";
            oLogin.email = email;
            oLogin.password = password;
            string ResToken = "";
            JSON = JsonConvert.SerializeObject(oLogin);
            Console.WriteLine("****JSON******" + JSON);

            //JSON = "{'email':'fasty.entel@insolutions.pe','password':'3Qv6M8#@w$N97Kqr'}";
            //JSON = "{'email':'admindev@entel.pe','password':'vBE8!r36DT@sYhFt'}";

            using (var stringContent = new
          StringContent(JSON,
      System.Text.Encoding.UTF8, "application/json"))
            using (var client = new HttpClient())
            {

                Console.WriteLine("****API REST******");
                var response = client.PostAsync(Api, stringContent);
                var result = response.Result;
                sResultado = result.ReasonPhrase;
                var contex = response.Result.Content.ReadAsStringAsync();
                ResultadoPost oResultado = JsonConvert.DeserializeObject<ResultadoPost>(contex.Result.ToString());
                ResToken = oResultado.token;

                Console.WriteLine("****TOKEN******" + ResToken);
                //Cadena = Token;
                //Console.ReadLine();

            }

            return ResToken;
        }


        public static RespuestaPostEnvio fn_GenerarCupon(
                    string Api_envio, string ResToken, string pcanal, string pfechaVencimiento, string pImportePendiente,
                    string pidCliente, string pSocio, string pidOperacion, string codConvenio)
        {
            RespuestaPostEnvio oResultado = new RespuestaPostEnvio();
            string JSON = "";
            //JSON = "{""canal"":""OP"",""codConvenio"":13003,""codOperacion"":2010,""empresaOrigen"":""Entel"",""fechaVencimiento"":""2021-05-30"",""idCliente"":""TESTDESARROLLO"",""idOperacion"":2022,""importe"":1,""moneda"":""PEN"",""nombreCliente"":""Contoso""}";
            string sResultado = "";
            EnvioPost oEnvioPost = new EnvioPost();
            oEnvioPost.canal = pcanal;//pcanal; CANAL DE OPERACIONES
            //oEnvioPost.codConvenio = "13003"; //convenio ISOLUTIONS CON ENTEL 13004 PROD
            oEnvioPost.codConvenio = codConvenio;
            oEnvioPost.codOperacion = "2010"; // AMBOS
            oEnvioPost.empresaOrigen = "Entel";//AMBOS
            string[] sFecha = pfechaVencimiento.Split(@"/");
            //string[] sFecha = new string[2];
            oEnvioPost.fechaVencimiento = (sFecha[2] + "-" + sFecha[1] + "-" + sFecha[0]);
            oEnvioPost.idCliente = pidCliente;
            oEnvioPost.idOperacion = pidOperacion.Replace("-", "");
            oEnvioPost.importe = Convert.ToDecimal(pImportePendiente.Replace(",", "")).ToString();
            oEnvioPost.moneda = "PEN";
            oEnvioPost.nombreCliente = pSocio;

            JSON = JsonConvert.SerializeObject(oEnvioPost);

            Console.WriteLine("****JSON ENVIO ******" + JSON);
            Console.WriteLine("****API REST ENVIO ******" + Api_envio);
            Console.WriteLine("****Enviando Serializacion con TOKEN....******" + Api_envio);

            using (var stringContent = new
             StringContent(JSON,
         System.Text.Encoding.UTF8, "application/json"))
            using (var client = new HttpClient())
            {
                client.DefaultRequestHeaders.Authorization = new
                    AuthenticationHeaderValue("Bearer", ResToken);

                Console.WriteLine("****Obteniendo respuesta POST Async....******" + Api_envio);

                var response = client.PostAsync(Api_envio, stringContent);

                //var result = await response.Content.ReadAsStringAsync();
                var result = response.Result;
                sResultado = result.ReasonPhrase;
                var contex = response.Result.Content.ReadAsStringAsync();

                oResultado = JsonConvert.DeserializeObject<RespuestaPostEnvio>(contex.Result.ToString());

                Console.WriteLine("****Transformando Respuesta a DeserializeObject RespuestaPostEnvio ******");
                Console.WriteLine("****transaccionId******" + oResultado.transaccionId);
                Console.WriteLine("****id******" + oResultado.id);
                Console.WriteLine("****fechaRegistro******" + oResultado.fechaRegistro);
                Console.WriteLine("****message******" + oResultado.message);
                //Console.ReadLine();
            }

            //oResultado.transaccionId = "C410"+ DateTime.Now.ToShortDateString(); ;
            return oResultado;
        }

        public static void fn_ActualizarResumen(string pID, string pTOPREGISTROS, string sHoja, string pPeriodo, string sBase)
        {
            string sQueryExcel = "";
            string sQUERY_ORACLE = "";
            DataTable firstTable_total = new DataTable(); ;
            System.Data.DataSet DtSet;
            DtSet = new System.Data.DataSet();
            string sAño = "";
            string sMes = "";
            string sMesText = "";

            if (pID == "1")
            {
                sQueryExcel = "select * from " + sHoja + " where PERIODO ='" + pPeriodo + "'";
                /*  EJECUTAMOS */
                //string sQUERY_VALIDACION = "SELECT C_CONTRATO FROM " + sEsquema + "\"TDAS_HISTORICO\" WHERE C_CONTRATO='" + oRows["C_CONTRATO"] + "'";
                sQUERY_ORACLE = "SELECT C_CONTRATO FROM " + sEsquema + "\"GGSS_HISTORICO\"  WHERE PERIODO ='" + pPeriodo + "'";
            }
            else if (pID == "3")
            {
                sQueryExcel = "select * from " + sHoja + " where PERIODO ='" + pPeriodo + "'"; //"and [C_CONTRATO] NOT IN(" + C_CONTRATO_FILTRO + ")";
                                                                                               //sQueryExcel = "select " + pTOPREGISTROS + " * from " + sHoja + " where PERIODO ='" + pPeriodo + "' and [C_CONTRATO] NOT IN(" + C_CONTRATO_FILTRO + ")";

                //using (System.Data.OleDb.OleDbConnection oOleDbConnection = new System.Data.OleDb.OleDbConnection("Provider=Microsoft.ACE.OLEDB.12.0;Data Source='" + sBase + "';Extended Properties=excel 12.0;"))
                //fn_EjecutarTiendas(pPeriodo);
                //sQUERY_VALIDACION = "SELECT C_CONTRATO FROM " + sEsquema + "\"TLV_HISTORICO\"  WHERE PERIODO ='" + pPeriodo + "'";
                sQUERY_ORACLE = "SELECT C_CONTRATO FROM " + sEsquema + "\"TLV_HISTORICO\" WHERE PERIODO ='" + pPeriodo + "'";


            }
            else if (pID == "4")
            {
                sAño = pPeriodo.Substring(0, 4);
                sMes = pPeriodo.Substring(sAño.Length, 2);
                sMesText = "";

                switch (sMes)
                {
                    case "01": sMesText = "ENERO"; break;
                    case "02": sMesText = "FEBRERO"; break;
                    case "03": sMesText = "MARZO"; break;
                    case "04": sMesText = "ABRIL"; break;
                    case "05": sMesText = "MAYO"; break;
                    case "06": sMesText = "JUNIO"; break;
                    case "07": sMesText = "JULIO"; break;
                    case "08": sMesText = "AGOSTO"; break;
                    case "09": sMesText = "SEPTIEMBRE"; break;
                    case "10": sMesText = "OCTUBRE"; break;
                    case "11": sMesText = "NOVIEMBRE"; break;
                    case "12": sMesText = "DICIEMBRE"; break;
                    default:
                        sMesText = "99";
                        break;
                }

                sQueryExcel = "select * from " + sHoja + " where [AÑO] =" + sAño + " and [MES]= '" + sMesText + "'";

                //string sQUERY = "SELECT C_CONTRATO FROM " + sEsquema + "\"TDAS_HISTORICO\" WHERE  AÑO  ='" + sAño + "' and  MES = '" + sMesText + "'";
                sQUERY_ORACLE = "SELECT C_CONTRATO FROM " + sEsquema + "\"TDAS_HISTORICO\" WHERE  AÑO  ='" + sAño + "' and  MES = '" + sMesText + "'";
                //fn_EjecutarTeleventas(pPeriodo);
            }

            /*  TOTALIZADO DEL ORACLE  */
            Console.WriteLine("sQUERY_ORACLE " + sQUERY_ORACLE);

            DataTable oObjArchivos_TOTALORACLE = new DataTable();
            using (OracleDataAdapter adp = new OracleDataAdapter(sQUERY_ORACLE, sConexion))
            {
                adp.Fill(oObjArchivos_TOTALORACLE);//all the data in OracleAdapter will be filled into Datatable 
            }

         //   Console.WriteLine("oObjArchivos_TOTALORACLE.Rows.Count " + oObjArchivos_TOTALORACLE.Rows.Count);
           // Console.WriteLine("pID" + pID + "/firstTable_total.Rows.Count:" + firstTable_total.Rows.Count + "oObjArchivos_HISTORICOS_VALIDACION.Rows.Count" + oObjArchivos_TOTALORACLE.Rows.Count + "pPeriodo" + pPeriodo);

            /*  FIN TOTALIZADO DEL ORACLE  */

            /*  TOTALIZADO DEL EXCEL    */
            using (System.Data.OleDb.OleDbConnection oOleDbConnection = new System.Data.OleDb.OleDbConnection("Provider = Microsoft.ACE.OLEDB.12.0; Data Source='" + sBase + "';Extended Properties = \"Excel 12.0 Xml; HDR = YES\";"))
            {
                oOleDbConnection.Open();
                using (System.Data.OleDb.OleDbDataAdapter oOleDbDataAdapterTotal =
        new System.Data.OleDb.OleDbDataAdapter(sQueryExcel, oOleDbConnection))
                {

                    Console.WriteLine("Obteniendo registros de la hoja  " + sHoja + "............");
                    //  Console.Read();

                    oOleDbDataAdapterTotal.TableMappings.Add("Table", "TestTable");
                    DtSet = new System.Data.DataSet();
                    oOleDbDataAdapterTotal.Fill(DtSet);

                    firstTable_total = DtSet.Tables[0];
                    

                }
            }
            /*  FIN TOTALIZADO DEL EXCEL    */
            Console.WriteLine("Total Registros obtenidos excel " + firstTable_total.Rows.Count);
            Console.WriteLine("Total Registros obtenidos oracle " + oObjArchivos_TOTALORACLE.Rows.Count);
            DBCGeneric oDbGeneric = new DBCGeneric();
            oDbGeneric.fn_AdicionarObjeto("PA_MIGRACIONTRANSFERENCIA_Actualizar", pID, oObjArchivos_TOTALORACLE.Rows.Count, 
                firstTable_total.Rows.Count, pPeriodo);


        }

        public static void SaveUsingOracleBulkCopy(DataTable dt)
        {
            string sQUERY = "";
            try
            {

                using (var connection = new OracleConnection(sConexion))
                {
                    connection.Open();
                    string[] PERIODO = new string[dt.Rows.Count];
                    string[] FECHAPROCESO = new string[dt.Rows.Count];
                    string[] FECHAACTIVACION = new string[dt.Rows.Count];
                    string[] RAZONSOCIAL = new string[dt.Rows.Count];
                    string[] C_CONTRATO = new string[dt.Rows.Count];
                    string[] TELEFONO = new string[dt.Rows.Count];
                    string[] MODELOEQUIPO = new string[dt.Rows.Count];
                    string[] N_PLAN = new string[dt.Rows.Count];
                    string[] VENDEDOR = new string[dt.Rows.Count];
                    string[] TIPODOCUMENTO = new string[dt.Rows.Count];
                    string[] DOCUMENTO = new string[dt.Rows.Count];
                    string[] NRO_ORDEN = new string[dt.Rows.Count];
                    string[] RENTABASICA = new string[dt.Rows.Count];
                    string[] VENDEDOR_PACKSIM = new string[dt.Rows.Count];
                    string[] PORTA_CEDENTE = new string[dt.Rows.Count];
                    string[] SISTEMAFUENTE = new string[dt.Rows.Count];
                    string[] LLAA_BASE_CAPTURA = new string[dt.Rows.Count];
                    string[] C_CONTRATOFS = new string[dt.Rows.Count];
                    string[] CODIGOBSCS = new string[dt.Rows.Count];
                    string[] VENDEDORDNI = new string[dt.Rows.Count];
                    string[] FLAG_PRODUCTO = new string[dt.Rows.Count];
                    string[] FLAGT0 = new string[dt.Rows.Count];
                    string[] FLAG_FT = new string[dt.Rows.Count];
                    string[] DESACTIVADOS = new string[dt.Rows.Count];
                    string[] CANAL = new string[dt.Rows.Count];
                    string[] SOCIO = new string[dt.Rows.Count];
                    string[] PUNTO_DE_VENTA_NORMALIZADO = new string[dt.Rows.Count];
                    string[] RENTA_IGV = new string[dt.Rows.Count];
                    string[] TIPO_DOCUMENTO_MERCADO = new string[dt.Rows.Count];
                    string[] MODO_PAGO = new string[dt.Rows.Count];
                    string[] TIPO_DE_VENTA = new string[dt.Rows.Count];
                    string[] MODO_ORIGEN = new string[dt.Rows.Count];
                    string[] PACK_SIM = new string[dt.Rows.Count];
                    string[] TECNOLOGIA = new string[dt.Rows.Count];
                    string[] TECNOLOGIA50 = new string[dt.Rows.Count];
                    string[] LLAA = new string[dt.Rows.Count];
                    string[] EASY_PACK = new string[dt.Rows.Count];
                    string[] PP50_FLEXIBLE = new string[dt.Rows.Count];
                    string[] AUTOACTIVADO_AA = new string[dt.Rows.Count];
                    string[] METRICA = new string[dt.Rows.Count];
                    string[] CONCATENAR = new string[dt.Rows.Count];
                    string[] SUB_CAJAS_ESQUEMA_GGSS_V50 = new string[dt.Rows.Count];
                    string[] CAJAS_ESQUEMA_GGSS = new string[dt.Rows.Count];
                    string[] CONSIDERA_CAJAS_ESQUEMA_SSNN = new string[dt.Rows.Count];
                    string[] CAJA_NUEVO_HISTORICO = new string[dt.Rows.Count];
                    string[] CLUSTERS = new string[dt.Rows.Count];
                    string[] CONIH_SINIH = new string[dt.Rows.Count];
                    string[] VEP_NOVEP = new string[dt.Rows.Count];
                    string[] SUPERVISOR_SSGG = new string[dt.Rows.Count];
                    string[] JEFE_DE_VENTAS_JV = new string[dt.Rows.Count];
                    string[] INCENTIVO_SSGG_MANDATO = new string[dt.Rows.Count];
                    string[] INCENTIVO_JJVV_MANDATO = new string[dt.Rows.Count];
                    string[] INCENTIVO_TOTAL_PROMOTOR = new string[dt.Rows.Count];
                    string[] UNITARIO_58 = new string[dt.Rows.Count];

                    for (int j = 0; j < dt.Rows.Count; j++)
                    {
                        PERIODO[j] = Convert.ToString(dt.Rows[j]["PERIODO"]);
                        FECHAPROCESO[j] = Convert.ToString(dt.Rows[j]["FECHAPROCESO"]);
                        FECHAACTIVACION[j] = Convert.ToString(dt.Rows[j]["FECHAACTIVACION"]);
                        RAZONSOCIAL[j] = Convert.ToString(dt.Rows[j]["RAZONSOCIAL"]);
                        C_CONTRATO[j] = Convert.ToString(dt.Rows[j]["C_CONTRATO"]);
                        TELEFONO[j] = Convert.ToString(dt.Rows[j]["TELEFONO"]);
                        MODELOEQUIPO[j] = Convert.ToString(dt.Rows[j]["MODELOEQUIPO"]);
                        N_PLAN[j] = Convert.ToString(dt.Rows[j]["N_PLAN"]);
                        VENDEDOR[j] = Convert.ToString(dt.Rows[j]["VENDEDOR"]);
                        TIPODOCUMENTO[j] = Convert.ToString(dt.Rows[j]["TIPODOCUMENTO"]);
                        DOCUMENTO[j] = Convert.ToString(dt.Rows[j]["DOCUMENTO"]);
                        NRO_ORDEN[j] = Convert.ToString(dt.Rows[j]["NRO_ORDEN"]);
                        RENTABASICA[j] = Convert.ToString(dt.Rows[j]["RENTABASICA"]);
                        VENDEDOR_PACKSIM[j] = Convert.ToString(dt.Rows[j]["VENDEDOR_PACKSIM"]);
                        PORTA_CEDENTE[j] = Convert.ToString(dt.Rows[j]["PORTA_CEDENTE"]);
                        SISTEMAFUENTE[j] = Convert.ToString(dt.Rows[j]["SISTEMAFUENTE"]);
                        LLAA_BASE_CAPTURA[j] = Convert.ToString(dt.Rows[j]["LLAA_BASE_CAPTURA"]);
                        C_CONTRATOFS[j] = Convert.ToString(dt.Rows[j]["C_CONTRATOFS"]);
                        CODIGOBSCS[j] = Convert.ToString(dt.Rows[j]["CODIGOBSCS"]);
                        VENDEDORDNI[j] = Convert.ToString(dt.Rows[j]["VENDEDORDNI"]);
                        FLAG_PRODUCTO[j] = Convert.ToString(dt.Rows[j]["FLAG_PRODUCTO"]);
                        FLAGT0[j] = Convert.ToString(dt.Rows[j]["FLAGT0"]);
                        FLAG_FT[j] = Convert.ToString(dt.Rows[j]["FLAG_FT"]);
                        DESACTIVADOS[j] = Convert.ToString(dt.Rows[j]["DESACTIVADOS"]);
                        CANAL[j] = Convert.ToString(dt.Rows[j]["CANAL"]);
                        SOCIO[j] = Convert.ToString(dt.Rows[j]["SOCIO"]);
                        PUNTO_DE_VENTA_NORMALIZADO[j] = Convert.ToString(dt.Rows[j]["PUNTO_DE_VENTA_NORMALIZADO"]);
                        RENTA_IGV[j] = Convert.ToString(dt.Rows[j]["RENTA_IGV"]);
                        TIPO_DOCUMENTO_MERCADO[j] = Convert.ToString(dt.Rows[j]["TIPO_DOCUMENTO_MERCADO"]);
                        MODO_PAGO[j] = Convert.ToString(dt.Rows[j]["MODO_PAGO"]);
                        TIPO_DE_VENTA[j] = Convert.ToString(dt.Rows[j]["TIPO_DE_VENTA"]);
                        MODO_ORIGEN[j] = Convert.ToString(dt.Rows[j]["MODO_ORIGEN"]);
                        PACK_SIM[j] = Convert.ToString(dt.Rows[j]["PACK_SIM"]);
                        TECNOLOGIA[j] = Convert.ToString(dt.Rows[j]["TECNOLOGIA"]);
                        TECNOLOGIA50[j] = Convert.ToString(dt.Rows[j]["TECNOLOGIA50"]);
                        LLAA[j] = Convert.ToString(dt.Rows[j]["LLAA"]);
                        EASY_PACK[j] = Convert.ToString(dt.Rows[j]["EASY_PACK"]);
                        PP50_FLEXIBLE[j] = Convert.ToString(dt.Rows[j]["PP50_FLEXIBLE"]);
                        AUTOACTIVADO_AA[j] = Convert.ToString(dt.Rows[j]["AUTOACTIVADO_AA"]);
                        METRICA[j] = Convert.ToString(dt.Rows[j]["METRICA"]);
                        CONCATENAR[j] = Convert.ToString(dt.Rows[j]["CONCATENAR"]);
                        SUB_CAJAS_ESQUEMA_GGSS_V50[j] = Convert.ToString(dt.Rows[j]["SUB_CAJAS_ESQUEMA_GGSS_V50"]);
                        CAJAS_ESQUEMA_GGSS[j] = Convert.ToString(dt.Rows[j]["CAJAS_ESQUEMA_GGSS"]);
                        CONSIDERA_CAJAS_ESQUEMA_SSNN[j] = Convert.ToString(dt.Rows[j]["CONSIDERA_CAJAS_ESQUEMA_SSNN"]);
                        CAJA_NUEVO_HISTORICO[j] = Convert.ToString(dt.Rows[j]["CAJA_NUEVO_HISTORICO"]);
                        CLUSTERS[j] = Convert.ToString(dt.Rows[j]["CLUSTERS"]);
                        CONIH_SINIH[j] = Convert.ToString(dt.Rows[j]["CONIH_SINIH"]);
                        VEP_NOVEP[j] = Convert.ToString(dt.Rows[j]["VEP_NOVEP"]);
                        SUPERVISOR_SSGG[j] = Convert.ToString(dt.Rows[j]["SUPERVISOR_SSGG"]);
                        JEFE_DE_VENTAS_JV[j] = Convert.ToString(dt.Rows[j]["JEFE_DE_VENTAS_JV"]);
                        INCENTIVO_SSGG_MANDATO[j] = Convert.ToString(dt.Rows[j]["INCENTIVO_SSGG_MANDATO"]);
                        INCENTIVO_JJVV_MANDATO[j] = Convert.ToString(dt.Rows[j]["INCENTIVO_JJVV_MANDATO"]);
                        INCENTIVO_TOTAL_PROMOTOR[j] = Convert.ToString(dt.Rows[j]["INCENTIVO_TOTAL_PROMOTOR"]);
                        UNITARIO_58[j] = Convert.ToString(dt.Rows[j]["UNITARIO_58"]);

                    }

                    OracleParameter P_PERIODO = new OracleParameter();
                    P_PERIODO.OracleDbType = OracleDbType.Varchar2;
                    P_PERIODO.Value = PERIODO;

                    OracleParameter P_FECHAPROCESO = new OracleParameter();
                    P_FECHAPROCESO.OracleDbType = OracleDbType.Varchar2;
                    P_FECHAPROCESO.Value = FECHAPROCESO;

                    OracleParameter P_FECHAACTIVACION = new OracleParameter();
                    P_FECHAACTIVACION.OracleDbType = OracleDbType.Varchar2;
                    P_FECHAACTIVACION.Value = FECHAACTIVACION;

                    OracleParameter P_RAZONSOCIAL = new OracleParameter();
                    P_RAZONSOCIAL.OracleDbType = OracleDbType.Varchar2;
                    P_RAZONSOCIAL.Value = RAZONSOCIAL;

                    OracleParameter P_C_CONTRATO = new OracleParameter();
                    P_C_CONTRATO.OracleDbType = OracleDbType.Varchar2;
                    P_C_CONTRATO.Value = C_CONTRATO;

                    OracleParameter P_TELEFONO = new OracleParameter();
                    P_TELEFONO.OracleDbType = OracleDbType.Varchar2;
                    P_TELEFONO.Value = TELEFONO;

                    OracleParameter P_MODELOEQUIPO = new OracleParameter();
                    P_MODELOEQUIPO.OracleDbType = OracleDbType.Varchar2;
                    P_MODELOEQUIPO.Value = MODELOEQUIPO;

                    OracleParameter P_N_PLAN = new OracleParameter();
                    P_N_PLAN.OracleDbType = OracleDbType.Varchar2;
                    P_N_PLAN.Value = N_PLAN;

                    OracleParameter P_VENDEDOR = new OracleParameter();
                    P_VENDEDOR.OracleDbType = OracleDbType.Varchar2;
                    P_VENDEDOR.Value = VENDEDOR;

                    OracleParameter P_TIPODOCUMENTO = new OracleParameter();
                    P_TIPODOCUMENTO.OracleDbType = OracleDbType.Varchar2;
                    P_TIPODOCUMENTO.Value = TIPODOCUMENTO;

                    OracleParameter P_DOCUMENTO = new OracleParameter();
                    P_DOCUMENTO.OracleDbType = OracleDbType.Varchar2;
                    P_DOCUMENTO.Value = DOCUMENTO;

                    OracleParameter P_NRO_ORDEN = new OracleParameter();
                    P_NRO_ORDEN.OracleDbType = OracleDbType.Varchar2;
                    P_NRO_ORDEN.Value = NRO_ORDEN;

                    OracleParameter P_RENTABASICA = new OracleParameter();
                    P_RENTABASICA.OracleDbType = OracleDbType.Varchar2;
                    P_RENTABASICA.Value = RENTABASICA;

                    OracleParameter P_VENDEDOR_PACKSIM = new OracleParameter();
                    P_VENDEDOR_PACKSIM.OracleDbType = OracleDbType.Varchar2;
                    P_VENDEDOR_PACKSIM.Value = VENDEDOR_PACKSIM;

                    OracleParameter P_PORTA_CEDENTE = new OracleParameter();
                    P_PORTA_CEDENTE.OracleDbType = OracleDbType.Varchar2;
                    P_PORTA_CEDENTE.Value = PORTA_CEDENTE;

                    OracleParameter P_SISTEMAFUENTE = new OracleParameter();
                    P_SISTEMAFUENTE.OracleDbType = OracleDbType.Varchar2;
                    P_SISTEMAFUENTE.Value = SISTEMAFUENTE;

                    OracleParameter P_LLAA_BASE_CAPTURA = new OracleParameter();
                    P_LLAA_BASE_CAPTURA.OracleDbType = OracleDbType.Varchar2;
                    P_LLAA_BASE_CAPTURA.Value = LLAA_BASE_CAPTURA;

                    OracleParameter P_C_CONTRATOFS = new OracleParameter();
                    P_C_CONTRATOFS.OracleDbType = OracleDbType.Varchar2;
                    P_C_CONTRATOFS.Value = C_CONTRATOFS;

                    OracleParameter P_CODIGOBSCS = new OracleParameter();
                    P_CODIGOBSCS.OracleDbType = OracleDbType.Varchar2;
                    P_CODIGOBSCS.Value = CODIGOBSCS;

                    OracleParameter P_VENDEDORDNI = new OracleParameter();
                    P_VENDEDORDNI.OracleDbType = OracleDbType.Varchar2;
                    P_VENDEDORDNI.Value = VENDEDORDNI;

                    OracleParameter FLAG_PRODUCTO_P = new OracleParameter();
                    FLAG_PRODUCTO_P.OracleDbType = OracleDbType.Varchar2;
                    FLAG_PRODUCTO_P.Value = FLAG_PRODUCTO;

                    OracleParameter FLAGT0_P = new OracleParameter();
                    FLAGT0_P.OracleDbType = OracleDbType.Varchar2;
                    FLAGT0_P.Value = FLAGT0;

                    OracleParameter FLAG_FT_P = new OracleParameter();
                    FLAG_FT_P.OracleDbType = OracleDbType.Varchar2;
                    FLAG_FT_P.Value = FLAG_FT;

                    OracleParameter DESACTIVADOS_P = new OracleParameter();
                    DESACTIVADOS_P.OracleDbType = OracleDbType.Varchar2;
                    DESACTIVADOS_P.Value = DESACTIVADOS;

                    OracleParameter CANAL_P = new OracleParameter();
                    CANAL_P.OracleDbType = OracleDbType.Varchar2;
                    CANAL_P.Value = CANAL;

                    OracleParameter SOCIO_P = new OracleParameter();
                    SOCIO_P.OracleDbType = OracleDbType.Varchar2;
                    SOCIO_P.Value = SOCIO;

                    OracleParameter PUNTO_DE_VENTA_NORMALIZADO_P = new OracleParameter();
                    PUNTO_DE_VENTA_NORMALIZADO_P.OracleDbType = OracleDbType.Varchar2;
                    PUNTO_DE_VENTA_NORMALIZADO_P.Value = PUNTO_DE_VENTA_NORMALIZADO;

                    OracleParameter RENTA_IGV_P = new OracleParameter();
                    RENTA_IGV_P.OracleDbType = OracleDbType.Varchar2;
                    RENTA_IGV_P.Value = RENTA_IGV;

                    OracleParameter TIPO_DOCUMENTO_MERCADO_P = new OracleParameter();
                    TIPO_DOCUMENTO_MERCADO_P.OracleDbType = OracleDbType.Varchar2;
                    TIPO_DOCUMENTO_MERCADO_P.Value = TIPO_DOCUMENTO_MERCADO;

                    OracleParameter MODO_PAGO_P = new OracleParameter();
                    MODO_PAGO_P.OracleDbType = OracleDbType.Varchar2;
                    MODO_PAGO_P.Value = MODO_PAGO;

                    OracleParameter TIPO_DE_VENTA_P = new OracleParameter();
                    TIPO_DE_VENTA_P.OracleDbType = OracleDbType.Varchar2;
                    TIPO_DE_VENTA_P.Value = TIPO_DE_VENTA;

                    OracleParameter MODO_ORIGEN_P = new OracleParameter();
                    MODO_ORIGEN_P.OracleDbType = OracleDbType.Varchar2;
                    MODO_ORIGEN_P.Value = MODO_ORIGEN;

                    OracleParameter PACK_SIM_P = new OracleParameter();
                    PACK_SIM_P.OracleDbType = OracleDbType.Varchar2;
                    PACK_SIM_P.Value = PACK_SIM;

                    OracleParameter TECNOLOGIA_P = new OracleParameter();
                    TECNOLOGIA_P.OracleDbType = OracleDbType.Varchar2;
                    TECNOLOGIA_P.Value = TECNOLOGIA;

                    OracleParameter TECNOLOGIA50_P = new OracleParameter();
                    TECNOLOGIA50_P.OracleDbType = OracleDbType.Varchar2;
                    TECNOLOGIA50_P.Value = TECNOLOGIA50;

                    OracleParameter LLAA_P = new OracleParameter();
                    LLAA_P.OracleDbType = OracleDbType.Varchar2;
                    LLAA_P.Value = LLAA;

                    OracleParameter EASY_PACK_P = new OracleParameter();
                    EASY_PACK_P.OracleDbType = OracleDbType.Varchar2;
                    EASY_PACK_P.Value = EASY_PACK;


                    OracleParameter PP50_FLEXIBLE_P = new OracleParameter();
                    PP50_FLEXIBLE_P.OracleDbType = OracleDbType.Varchar2;
                    PP50_FLEXIBLE_P.Value = PP50_FLEXIBLE;

                    OracleParameter AUTOACTIVADO_AA_P = new OracleParameter();
                    AUTOACTIVADO_AA_P.OracleDbType = OracleDbType.Varchar2;
                    AUTOACTIVADO_AA_P.Value = AUTOACTIVADO_AA;

                    OracleParameter METRICA_P = new OracleParameter();
                    METRICA_P.OracleDbType = OracleDbType.Varchar2;
                    METRICA_P.Value = METRICA;

                    OracleParameter CONCATENAR_P = new OracleParameter();
                    CONCATENAR_P.OracleDbType = OracleDbType.Varchar2;
                    CONCATENAR_P.Value = CONCATENAR;

                    OracleParameter SUB_CAJAS_ESQUEMA_GGSS_V50_P = new OracleParameter();
                    SUB_CAJAS_ESQUEMA_GGSS_V50_P.OracleDbType = OracleDbType.Varchar2;
                    SUB_CAJAS_ESQUEMA_GGSS_V50_P.Value = SUB_CAJAS_ESQUEMA_GGSS_V50;

                    OracleParameter CAJAS_ESQUEMA_GGSS_P = new OracleParameter();
                    CAJAS_ESQUEMA_GGSS_P.OracleDbType = OracleDbType.Varchar2;
                    CAJAS_ESQUEMA_GGSS_P.Value = CAJAS_ESQUEMA_GGSS;

                    OracleParameter CONSIDERA_CAJAS_ESQUEMA_SSNN_P = new OracleParameter();
                    CONSIDERA_CAJAS_ESQUEMA_SSNN_P.OracleDbType = OracleDbType.Varchar2;
                    CONSIDERA_CAJAS_ESQUEMA_SSNN_P.Value = CONSIDERA_CAJAS_ESQUEMA_SSNN;

                    OracleParameter CAJA_NUEVO_HISTORICO_P = new OracleParameter();
                    CAJA_NUEVO_HISTORICO_P.OracleDbType = OracleDbType.Varchar2;
                    CAJA_NUEVO_HISTORICO_P.Value = CAJA_NUEVO_HISTORICO;

                    OracleParameter CLUSTERS_P = new OracleParameter();
                    CLUSTERS_P.OracleDbType = OracleDbType.Varchar2;
                    CLUSTERS_P.Value = CLUSTERS;

                    OracleParameter CONIH_SINIH_P = new OracleParameter();
                    CONIH_SINIH_P.OracleDbType = OracleDbType.Varchar2;
                    CONIH_SINIH_P.Value = CONIH_SINIH;

                    OracleParameter VEP_NOVEP_P = new OracleParameter();
                    VEP_NOVEP_P.OracleDbType = OracleDbType.Varchar2;
                    VEP_NOVEP_P.Value = VEP_NOVEP;

                    OracleParameter SUPERVISOR_SSGG_P = new OracleParameter();
                    SUPERVISOR_SSGG_P.OracleDbType = OracleDbType.Varchar2;
                    SUPERVISOR_SSGG_P.Value = SUPERVISOR_SSGG;

                    OracleParameter JEFE_DE_VENTAS_JV_P = new OracleParameter();
                    JEFE_DE_VENTAS_JV_P.OracleDbType = OracleDbType.Varchar2;
                    JEFE_DE_VENTAS_JV_P.Value = JEFE_DE_VENTAS_JV;

                    OracleParameter INCENTIVO_SSGG_MANDATO_P = new OracleParameter();
                    INCENTIVO_SSGG_MANDATO_P.OracleDbType = OracleDbType.Varchar2;
                    INCENTIVO_SSGG_MANDATO_P.Value = INCENTIVO_SSGG_MANDATO;

                    OracleParameter INCENTIVO_JJVV_MANDATO_P = new OracleParameter();
                    INCENTIVO_JJVV_MANDATO_P.OracleDbType = OracleDbType.Varchar2;
                    INCENTIVO_JJVV_MANDATO_P.Value = INCENTIVO_JJVV_MANDATO;

                    OracleParameter INCENTIVO_TOTAL_PROMOTOR_P = new OracleParameter();
                    INCENTIVO_TOTAL_PROMOTOR_P.OracleDbType = OracleDbType.Varchar2;
                    INCENTIVO_TOTAL_PROMOTOR_P.Value = INCENTIVO_TOTAL_PROMOTOR;

                    OracleParameter UNITARIO_58_P = new OracleParameter();
                    UNITARIO_58_P.OracleDbType = OracleDbType.Varchar2;
                    UNITARIO_58_P.Value = UNITARIO_58;

                    sQUERY = "INSERT INTO " + sEsquema + "GGSS_HISTORICO  (" +
                    "PERIODO                  , " +
"FECHAPROCESO             , " +
"FECHAACTIVACION           , " +
"RAZONSOCIAL                , " +
"C_CONTRATO                           , " +
"TELEFONO                             , " +
"MODELOEQUIPO                         , " +
"N_PLAN                               , " +
"VENDEDOR                             , " +
"TIPODOCUMENTO                        , " +
"DOCUMENTO                            , " +
"NRO_ORDEN                           , " +
"RENTABASICA                         , " +
"VENDEDOR_PACKSIM                    , " +
"PORTA_CEDENTE                       , " +
"SISTEMAFUENTE                       , " +
"LLAA_BASE_CAPTURA                   , " +
"C_CONTRATOFS                        , " +
"CODIGOBSCS                          , " +
"VENDEDORDNI                         , " +
"FLAG_PRODUCTO                       , " +
"FLAGT0                              , " +
"FLAG_FT                             , " +
"DESACTIVADOS                        , " +
"CANAL                               , " +
"SOCIO                               , " +
"PUNTO_DE_VENTA_NORMALIZADO          , " +
"RENTA_IGV                           , " +
"TIPO_DOCUMENTO_MERCADO              , " +
"MODO_PAGO                           , " +
"TIPO_DE_VENTA                       , " +
"MODO_ORIGEN                         , " +
"PACK_SIM                            , " +
"TECNOLOGIA                          , " +
"TECNOLOGIA50                        , " +
"LLAA                                , " +
"EASY_PACK                           , " +
"PP50_FLEXIBLE                       , " +
"AUTOACTIVADO_AA                     , " +
"METRICA                             , " +
"CONCATENAR                          , " +
"SUB_CAJAS_ESQUEMA_GGSS_V50          , " +
"CAJAS_ESQUEMA_GGSS                  , " +
"CONSIDERA_CAJAS_ESQUEMA_SSNN        , " +
"CAJA_NUEVO_HISTORICO                , " +
"CLUSTERS                            , " +
"CONIH_SINIH                         , " +
"VEP_NOVEP                           , " +
"SUPERVISOR_SSGG                     , " +
"JEFE_DE_VENTAS_JV                   , " +
"INCENTIVO_SSGG_MANDATO              , " +
"INCENTIVO_JJVV_MANDATO              , " +
"INCENTIVO_TOTAL_PROMOTOR            , " +
"UNITARIO_58) VALUES (:1, :2, :3 , :4, :5 ,:6 , :7 , :8 , :9 , :10, :11 ,:12 , :13 , :14 , :15 , :16, :17 ,:18 , :19 , :20, :21 , :22, :23 ,:24 , :25 , :26, :27 , :28, :29 , :30, :31 , 32 , :33 , :34 , :35 , :36 , :37 , :38 , :39 , : 40 , : 41 , :42 , :43 , :44 , :45 , :46, :47, :48, :49, :50 , : 51 , : 52 , : 53 , : 54 ) ";


                    // create command and set properties
                    OracleCommand cmd = connection.CreateCommand();
                    //cmd.CommandText = "INSERT INTO " + sEsquema + "\"GGSS_HISTORICO\" (ID, NAME, ADDRESS) VALUES (:1, :2, :3)";
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = sQUERY;
                    //cmd.ArrayBindCount = PERIODO.Length;
                    cmd.Parameters.Add(P_PERIODO);//1
                    cmd.Parameters.Add(P_FECHAPROCESO); //2
                    cmd.Parameters.Add(P_FECHAACTIVACION); //3
                    cmd.Parameters.Add(P_RAZONSOCIAL); //4
                    cmd.Parameters.Add(P_C_CONTRATO); //5
                    cmd.Parameters.Add(P_TELEFONO);//6
                    cmd.Parameters.Add(P_MODELOEQUIPO); //7
                    cmd.Parameters.Add(P_N_PLAN);//8
                    cmd.Parameters.Add(P_VENDEDOR); //9
                    cmd.Parameters.Add(P_TIPODOCUMENTO); //10
                    cmd.Parameters.Add(P_DOCUMENTO);//11
                    cmd.Parameters.Add(P_NRO_ORDEN);//12
                    cmd.Parameters.Add(P_RENTABASICA);//13
                    cmd.Parameters.Add(P_VENDEDOR_PACKSIM);//14
                    cmd.Parameters.Add(P_PORTA_CEDENTE);//15
                    cmd.Parameters.Add(P_SISTEMAFUENTE);//16
                    cmd.Parameters.Add(P_LLAA_BASE_CAPTURA);//17
                    cmd.Parameters.Add(P_C_CONTRATOFS);//18
                    cmd.Parameters.Add(P_CODIGOBSCS);//19
                    cmd.Parameters.Add(P_VENDEDORDNI);//20
                    cmd.Parameters.Add(FLAG_PRODUCTO_P);//21
                    cmd.Parameters.Add(FLAGT0_P);//22
                    cmd.Parameters.Add(FLAG_FT_P);//23
                    cmd.Parameters.Add(DESACTIVADOS_P);//24
                    cmd.Parameters.Add(CANAL_P);//25
                    cmd.Parameters.Add(SOCIO_P);//26
                    cmd.Parameters.Add(PUNTO_DE_VENTA_NORMALIZADO_P); //27
                    cmd.Parameters.Add(RENTA_IGV_P);//28
                    cmd.Parameters.Add(TIPO_DOCUMENTO_MERCADO_P); //29
                    cmd.Parameters.Add(MODO_PAGO_P); //30
                    cmd.Parameters.Add(TIPO_DE_VENTA_P);//31
                    cmd.Parameters.Add(MODO_ORIGEN_P);//32
                    cmd.Parameters.Add(PACK_SIM_P);//33
                    cmd.Parameters.Add(TECNOLOGIA_P); //34
                    cmd.Parameters.Add(TECNOLOGIA50_P);//35
                    cmd.Parameters.Add(LLAA_P);//36
                    cmd.Parameters.Add(EASY_PACK_P); //37
                    cmd.Parameters.Add(PP50_FLEXIBLE_P); //38
                    cmd.Parameters.Add(AUTOACTIVADO_AA_P); //39
                    cmd.Parameters.Add(METRICA_P); //40
                    cmd.Parameters.Add(CONCATENAR_P); //41
                    cmd.Parameters.Add(SUB_CAJAS_ESQUEMA_GGSS_V50_P); //42
                    cmd.Parameters.Add(CAJAS_ESQUEMA_GGSS_P); //43
                    cmd.Parameters.Add(CONSIDERA_CAJAS_ESQUEMA_SSNN_P);//44
                    cmd.Parameters.Add(CAJA_NUEVO_HISTORICO_P); //45
                    cmd.Parameters.Add(CLUSTERS_P);//46
                    cmd.Parameters.Add(CONIH_SINIH_P); //47
                    cmd.Parameters.Add(VEP_NOVEP_P); //48
                    cmd.Parameters.Add(SUPERVISOR_SSGG_P); //49
                    cmd.Parameters.Add(JEFE_DE_VENTAS_JV_P); //50
                    cmd.Parameters.Add(INCENTIVO_SSGG_MANDATO_P);//51
                    cmd.Parameters.Add(INCENTIVO_JJVV_MANDATO_P); //52
                    cmd.Parameters.Add(INCENTIVO_TOTAL_PROMOTOR_P);//53
                    cmd.Parameters.Add(UNITARIO_58_P);//54
                    //cmd.BindByName = true;

                    cmd.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        static DataTable fn_ObtenerResultado(string pQuery)
        {
            DataTable oObj = new DataTable();
            using (OracleDataAdapter adp = new OracleDataAdapter(pQuery, sConexion))
            {
                adp.Fill(oObj);//all the data in OracleAdapter will be filled into Datatable 

            }
            return oObj;
        }

        static void fn_Env()
        {
            DataTable oObjArchivos = new DataTable();
            Console.WriteLine("INCIANDO CONSULTA 1");
            //string sQUERY = "SELECT ID,DESCRIPCION,NOMBREARCHIVO,RUTAARCHIVO,NOMBREHOJA,PERIODO,TOPREGISTROS FROM " + sEsquema + "\"ARCHIVOSTRANSFERENCIA\" WHERE ACTIVO = '1'";

            string sQUERY = "SELECT ID,DESCRIPCION,NOMBREARCHIVO,RUTAARCHIVO,NOMBREHOJA,PERIODO,TOPREGISTROS FROM " + sEsquema + "\"ARCHIVOSTRANSFERENCIA\" ";
            //string sQUERY = "select 1 from dual";
            Console.WriteLine("sQUERY: " + sQUERY);
            Console.WriteLine("sConexion: " + sConexion);

            using (OracleDataAdapter adp = new OracleDataAdapter(sQUERY, sConexion))
            {
                adp.Fill(oObjArchivos);//all the data in OracleAdapter will be filled into Datatable 
            }

            Console.WriteLine("TERMINANDO CONSULTA 1");
            List<Archivos> oArchivos = new List<Archivos>();

            foreach (DataRow oRows in oObjArchivos.Rows)
            {
                Archivos oArchivo = new Archivos();
                oArchivo.pNombreArchivo = oRows["NOMBREARCHIVO"].ToString();
                oArchivo.pPeriodo = oRows["PERIODO"].ToString();
                oArchivo.pFechaUtilmaCarga = DateTime.Now.ToShortDateString();
                oArchivo.pRegistrosCopiado = "0";
                oArchivo.pTotalRegistro = "0";
                //oRows["NOMBREHOJA"].ToString(), oRows["ID"].ToString(), oRows["PERIODO"].ToString(), oRows["TOPREGISTROS"].ToString(), SIDRegistro, oRows["NOMBREARCHIVO"].ToString());
                if (oArchivo != null)
                {
                    oArchivos.Add(oArchivo);
                }
            }

            /*
            if (oArchivos != null)
                fn_EnviarCorreo(oArchivos);

            */


        }

        static void fn_ActualizarSQL(string sBase, string sSql)
        {
            try
            {
                
                string sQueryExcel = "";

                string consStringINTERNET = ConfigurationManager.ConnectionStrings["CnxSql"].ConnectionString;
                int iColum = 0;
                    //using (System.Data.OleDb.OleDbConnection oOleDbConnection = new System.Data.OleDb.OleDbConnection("Provider=Microsoft.ACE.OLEDB.12.0;Data Source='" + sBase + "';Extended Properties=excel 12.0;"))
                    using (SqlConnection oOleDbConnection = new SqlConnection(EncriptacionMartin.MetodoEncriptacion.Desencriptar(consStringINTERNET)))
                {
                    oOleDbConnection.Open();
                    using (SqlCommand myCommand =
            new SqlCommand(sQueryExcel, oOleDbConnection))
                    {
                        string sql = sSql;//"Update [Pendientes_EECC$] set NRO_CUPON = '665544' where NRO_FACTURA= 700050128 ";

                        myCommand.CommandText = sql;
                        myCommand.ExecuteNonQuery();
                    }
                }

                //fn_ApiConsumer();
                //fn_EjecutarTransferencia();
            }
            catch (Exception EX)
            {

                Console.WriteLine(EX.Message);
            }

        }


        static void fn_ActualizarExcel(string sBase,string sSql)
        {
            try
            {
                string sQueryExcel = "";
                //using (System.Data.OleDb.OleDbConnection oOleDbConnection = new System.Data.OleDb.OleDbConnection("Provider=Microsoft.ACE.OLEDB.12.0;Data Source='" + sBase + "';Extended Properties=excel 12.0;"))
                using (System.Data.OleDb.OleDbConnection oOleDbConnection = new System.Data.OleDb.OleDbConnection("Provider = Microsoft.ACE.OLEDB.12.0; Data Source='" + sBase + "';Extended Properties = \"Excel 12.0 Xml; HDR = YES\";"))
                {
                    oOleDbConnection.Open();
                    using (System.Data.OleDb.OleDbCommand myCommand =
            new System.Data.OleDb.OleDbCommand(sQueryExcel, oOleDbConnection))
                    {
                        string sql = sSql;//"Update [Pendientes_EECC$] set NRO_CUPON = '665544' where NRO_FACTURA= 700050128 ";

                        myCommand.CommandText = sql;
                        myCommand.ExecuteNonQuery();
                    }
                }

                //fn_ApiConsumer();
                //fn_EjecutarTransferencia();
            }
            catch (Exception EX)
            {

                Console.WriteLine(EX.Message);
            }

        }

        static void fn_ObtenerValorStock_NOUSAR()
        {
            string SIDRegistro = "";
            try
            {

                //fn_Env();
                //return;

                /****************/
                DataTable oObjArchivos = new DataTable();
                Console.WriteLine("INICIANDO CONSULTA 1");

                string sQUERY = "SELECT ID,DESCRIPCION,NOMBREARCHIVO,RUTAARCHIVO,NOMBREHOJA,PERIODO,TOPREGISTROS FROM " + sEsquema + "\"ARCHIVOSTRANSFERENCIA\" WHERE ACTIVO = '1'";
                //string sQUERY = "SELECT ID,DESCRIPCION,NOMBREARCHIVO,RUTAARCHIVO,NOMBREHOJA,PERIODO,TOPREGISTROS FROM " + sEsquema + "\"ARCHIVOSTRANSFERENCIA\" WHERE ID = '3'";
                //string sQUERY = "select 1 from dual";
                Console.WriteLine("sQUERY: " + sQUERY);
                Console.WriteLine("sConexion: " + sConexion);

                using (OracleDataAdapter adp = new OracleDataAdapter(sQUERY, sConexion))
                {
                    adp.Fill(oObjArchivos);//all the data in OracleAdapter will be filled into Datatable 
                }

                Console.WriteLine("TERMINANDO CONSULTA 1");
                List<Archivos> oArchivos = new List<Archivos>();

                foreach (DataRow oRows in oObjArchivos.Rows)
                {
                    //SIDRegistro = fn_RegistroMigracion(oRows["DESCRIPCION"].ToString(), DateTime.Now.ToShortDateString(), "0", "0", oRows["NOMBREARCHIVO"].ToString(), DateTime.Now.ToShortDateString(), oRows["NOMBREHOJA"].ToString(), oRows["RUTAARCHIVO"].ToString(), DateTime.Now.ToShortTimeString(), "", "EN PROCESO", oRows["PERIODO"].ToString());
                    Archivos oArchivo = fn_VaciaExcel(oRows["RUTAARCHIVO"].ToString(), oRows["NOMBREHOJA"].ToString(), oRows["ID"].ToString(), oRows["PERIODO"].ToString(), oRows["TOPREGISTROS"].ToString(), SIDRegistro, oRows["NOMBREARCHIVO"].ToString());
                    if (oArchivo != null)
                    {
                        oArchivos.Add(oArchivo);
                    }

                    //Actualiza Resumen en totalidad
                    fn_ActualizarResumen(oRows["ID"].ToString(), oRows["TOPREGISTROS"].ToString(), oRows["NOMBREHOJA"].ToString(), oRows["PERIODO"].ToString(), oRows["RUTAARCHIVO"].ToString());
                }
                //if(oArchivos!=null)
                //fn_EnviarCorreo(oArchivos);

                fn_EjecutarMacro();
            }
            catch (Exception ex)
            {

                Console.WriteLine(ex.Message);
                Console.ReadLine();
                string sQueryUpdate = "update " + sEsquema + "\"MIGRACIONTRANSFERENCIA\" set ESTADO='ERROR' , ESTADODESCRIPCION='" + ex.Message + "' WHERE IDENTI ='" + SIDRegistro + "'";
                fn_Registrar(sQueryUpdate);

                Console.WriteLine(ex.Message);
                Console.ReadLine();
            }

        }



        static void Main(string[] args)
        {
            //fn_EjecutarTransferencia();

            try
            {
                fn_GenerarCupoNC();
                fn_GenerarFechaVencimiento_main();

                    /* 
                   fn_ActualizarEstadoCupon_Excel_Produccion_new();
                   fn_GenerarCuponPendientes();
                   fn_GenerarExcelActualizado();
                    */


                //fn_ApiConsumerOracleEECC();
                //fn_ApiConsumerOracle();
                //fn_ActualizarEstadoCupon_Excel_Produccion();
                //fn_ActualizarEstadoCupon_Excel_Produccion();

                //ACTUALIZAR ESTADO DE LOS CUPONES CUANDO ESTEN ANULADOS O PENDIENTES

                //fn_ApiConsumerOracle();
                //ANULAR CUPONES POR CADA 1 HORA
                //fn_AnularCupon();
                }
            catch (Exception EX)
            {
                
                Console.WriteLine("fn_ApiConsumerOracleEECC "  +EX.Message);
                //Console.ReadLine();


            }


        }


        static void fn_ValorStock_SQL_To_Oracle()
        {



        }


        static void fn_EjecutarTransferencia()
        {
            string SIDRegistro = "";
            try
            {

                //fn_Env();
                //return;

                /****************/
                DataTable oObjArchivos = new DataTable();
                Console.WriteLine("INICIANDO CONSULTA 1");

                string sQUERY = "SELECT ID,DESCRIPCION,NOMBREARCHIVO,RUTAARCHIVO,NOMBREHOJA,PERIODO,TOPREGISTROS FROM " + sEsquema + "\"ARCHIVOSTRANSFERENCIA\" WHERE ACTIVO = '1'";
                //string sQUERY = "SELECT ID,DESCRIPCION,NOMBREARCHIVO,RUTAARCHIVO,NOMBREHOJA,PERIODO,TOPREGISTROS FROM " + sEsquema + "\"ARCHIVOSTRANSFERENCIA\" WHERE ID = '3'";
                //string sQUERY = "select 1 from dual";
                Console.WriteLine("sQUERY: " + sQUERY);
                Console.WriteLine("sConexion: " + sConexion);

                using (OracleDataAdapter adp = new OracleDataAdapter(sQUERY, sConexion))
                {
                    adp.Fill(oObjArchivos);//all the data in OracleAdapter will be filled into Datatable 
                }

                Console.WriteLine("TERMINANDO CONSULTA 1");
                List<Archivos> oArchivos = new List<Archivos>();

                foreach (DataRow oRows in oObjArchivos.Rows)
                {
                    //SIDRegistro = fn_RegistroMigracion(oRows["DESCRIPCION"].ToString(), DateTime.Now.ToShortDateString(), "0", "0", oRows["NOMBREARCHIVO"].ToString(), DateTime.Now.ToShortDateString(), oRows["NOMBREHOJA"].ToString(), oRows["RUTAARCHIVO"].ToString(), DateTime.Now.ToShortTimeString(), "", "EN PROCESO", oRows["PERIODO"].ToString());
                    Archivos oArchivo = fn_VaciaExcel(oRows["RUTAARCHIVO"].ToString(), oRows["NOMBREHOJA"].ToString(), oRows["ID"].ToString(), oRows["PERIODO"].ToString(), oRows["TOPREGISTROS"].ToString(), SIDRegistro, oRows["NOMBREARCHIVO"].ToString());
                    if (oArchivo != null)
                    {
                        oArchivos.Add(oArchivo);
                    }

                    //Actualiza Resumen en totalidad
                    fn_ActualizarResumen(oRows["ID"].ToString(), oRows["TOPREGISTROS"].ToString(), oRows["NOMBREHOJA"].ToString(), oRows["PERIODO"].ToString(), oRows["RUTAARCHIVO"].ToString());
                }


                //if(oArchivos!=null)
                //fn_EnviarCorreo(oArchivos);

                fn_EjecutarMacro();
            }
            catch (Exception ex)
            {

                Console.WriteLine(ex.Message);
                Console.ReadLine();
                string sQueryUpdate = "update " + sEsquema + "\"MIGRACIONTRANSFERENCIA\" set ESTADO='ERROR' , ESTADODESCRIPCION='" + ex.Message + "' WHERE IDENTI ='" + SIDRegistro + "'";
                fn_Registrar(sQueryUpdate);

                Console.WriteLine(ex.Message);
                Console.ReadLine();
            }

        }

        static string fn_Registrar(string pQUERY)
        {
            using (var con = new Oracle.ManagedDataAccess.Client.OracleConnection(sConexion))
            {
                con.Open();

                OracleParameter id = new OracleParameter();
                id.OracleDbType = OracleDbType.Varchar2;
                id.Value = DateTime.Now.ToLongDateString();

                // create command and set properties
                OracleCommand cmd = con.CreateCommand();
                cmd.CommandText = pQUERY;  //"INSERT INTO BULKINSERTTEST (ID, NAME, ADDRESS) VALUES (:1, :2, :3)";
                                           //cmd.ArrayBindCount = ids.Length;
                                           //cmd.Parameters.Add(id);
                                           //cmd.Parameters.Add(name);
                                           //cmd.Parameters.Add(address);
                cmd.ExecuteNonQuery();

            }
            return "1";
        }

        static string fn_RegistroMigracion(string DESCRIPCION, string FECHAREGISTRO, string CANTIDADMIGRACION, string TOTALREGISTROSORIGEN, string NOMBREARCHIVO, string FECHAPROCESO, string HOJAARCHIVO, string RUTAARCHIVO, string HORAINICIO, string HORAFINAL, string ESTADO, string PERIODO)
        {
            string sID = "";
            //\"ARCHIVOSTRANSFERENCIA\"";
            //string sQUERY_1 = "SELECT ID FROM " + sEsquema + "\"MIGRACIONTRANSFERENCIA\" order by \"ID\" desc fetch  first 1 rows only ;";
            //string sQUERY_1 = "SELECT IDENTI FROM " + sEsquema + "\"MIGRACIONTRANSFERENCIA\" order by IDENTI desc fetch first 1 rows only ";            
            string sQUERY_1 = "SELECT NVL(MAX(ROWNUM),'0') IDENTI FROM " + sEsquema + "\"MIGRACIONTRANSFERENCIA\" ";
            Console.WriteLine("sQUERY_1 " + sQUERY_1);
            DataTable oMIGRACIONTRANSFERENCIA_U = fn_ObtenerResultado(sQUERY_1);
            //Console.WriteLine("COUNT IDENTI " + oMIGRACIONTRANSFERENCIA_U.Rows.Count);

            foreach (DataRow oRows2 in oMIGRACIONTRANSFERENCIA_U.Rows)
            {
                Console.WriteLine("IDENTI " + (Convert.ToInt32(oRows2["IDENTI"])).ToString());
            }

            foreach (DataRow oRows2 in oMIGRACIONTRANSFERENCIA_U.Rows)
            {
                sID = (Convert.ToInt32(oRows2["IDENTI"]) + 1).ToString();
            }

            Console.WriteLine("IDENTI NEW" + sID);
            //sID = (oMIGRACIONTRANSFERENCIA_U.Rows.Count + 1).ToString();

            if (sID == "")
            {
                sID = "0";

            }

            Console.WriteLine("TERMINANDO CONSULTA 2");

            string sQUERY = "INSERT INTO " + sEsquema + "\"MIGRACIONTRANSFERENCIA\" (IDENTI, DESCRIPCION,FECHAREGISTRO,CANTIDADMIGRACION,TOTALREGISTROSORIGEN,NOMBREARCHIVO,FECHAPROCESO,HOJAARCHIVO,RUTAARCHIVO,HORAINICIO,HORAFINAL,ESTADO,PERIODO)"
                + " VALUES ( '" + sID + "','" + DESCRIPCION + "','" + FECHAREGISTRO + "','" + CANTIDADMIGRACION + "','" + TOTALREGISTROSORIGEN + "','" +
                NOMBREARCHIVO + "','" +
                FECHAPROCESO + "','" +
                HOJAARCHIVO + "','" +
                RUTAARCHIVO + "','" +
                HORAINICIO + "','" +
                HORAFINAL + "','" +
                ESTADO + "','" +
                PERIODO + "')";

            Console.WriteLine("TERMINANDO QUERY 3: " + sQUERY);

            //HOJAARCHIVO,RUTAARCHIVO,HORAINICIO,HORAFINAL,ESTADO

            using (var con = new Oracle.ManagedDataAccess.Client.OracleConnection(sConexion))
            {
                con.Open();
                int[] foos = new int[3] { 1, 2, 3 };
                string[] bars = new string[3] { "A", "B", "C" };


                OracleParameter id = new OracleParameter();
                id.OracleDbType = OracleDbType.Varchar2;
                id.Value = DateTime.Now.ToLongDateString();
                /*
                OracleParameter name = new OracleParameter();
                name.OracleDbType = OracleDbType.Varchar2;
                name.Value = names;

                OracleParameter address = new OracleParameter();
                address.OracleDbType = OracleDbType.Varchar2;
                address.Value = addresses;
                */
                // create command and set properties
                OracleCommand cmd = con.CreateCommand();
                cmd.CommandText = sQUERY;  //"INSERT INTO BULKINSERTTEST (ID, NAME, ADDRESS) VALUES (:1, :2, :3)";
                                           //cmd.ArrayBindCount = ids.Length;
                                           //cmd.Parameters.Add(id);
                                           //cmd.Parameters.Add(name);
                                           //cmd.Parameters.Add(address);
                cmd.ExecuteNonQuery();

                /*
                 DataTable oMIGRACIONTRANSFERENCIA = fn_ObtenerResultado("SELECT IDENTI FROM " + sEsquema + "\"MIGRACIONTRANSFERENCIA\" WHERE ROWNUM <2 order by IDENTI  desc");
                  
                foreach (DataRow oRows in oMIGRACIONTRANSFERENCIA.Rows)
                {
                    sID = oRows["IDENTI"].ToString();
                }*/
            }


            return sID;

        }


        static void fn_OracleConexion4(
            TransferenciaParametro4 pTransferenciaParametro
            )
        {
            try
            {


                string sQUERY = "";
                DataTable dt = new DataTable();

                sQUERY = "INSERT INTO " + sEsquema + "TDAS_HISTORICO  (" +
                    "C_CONTRATO," +
"FECHAPROCESO," +
"N_PLAN," +
"ESTADOINAR," +
"RENTAIGV," +
"MODOPAGO," +
"TIPOVENTA," +
"MODEL_F," +
"SOCIO," +
"VISTA_METRICA," +
"MONTO_COMISION," +
"MES," +
"AÑO " + ") VALUES (" +
"'" + pTransferenciaParametro.C_CONTRATO + "'," +
"'" + pTransferenciaParametro.FECHAPROCESO + "'," +
"'" + pTransferenciaParametro.N_PLAN + "'," +
"'" + pTransferenciaParametro.ESTADOINAR + "'," +
"'" + pTransferenciaParametro.RENTAIGV + "'," +
"'" + pTransferenciaParametro.MODOPAGO + "'," +
"'" + pTransferenciaParametro.TIPOVENTA + "'," +
"'" + pTransferenciaParametro.MODEL_F + "'," +
"'" + pTransferenciaParametro.SOCIO + "'," +
"'" + pTransferenciaParametro.VISTA_METRICA + "'," +
"'" + pTransferenciaParametro.MONTO_COMISION + "'," +
"'" + pTransferenciaParametro.MES + "'," +
"'" + pTransferenciaParametro.AÑO + "'" + ")";

                using (var con = new Oracle.ManagedDataAccess.Client.OracleConnection(sConexion))
                {
                    con.Open();
                    int[] foos = new int[3] { 1, 2, 3 };
                    string[] bars = new string[3] { "A", "B", "C" };


                    OracleParameter id = new OracleParameter();
                    id.OracleDbType = OracleDbType.Varchar2;
                    id.Value = pTransferenciaParametro.C_CONTRATO;
                    /*
                    OracleParameter name = new OracleParameter();
                    name.OracleDbType = OracleDbType.Varchar2;
                    name.Value = names;

                    OracleParameter address = new OracleParameter();
                    address.OracleDbType = OracleDbType.Varchar2;
                    address.Value = addresses;
                    */
                    // create command and set properties
                    OracleCommand cmd = con.CreateCommand();
                    cmd.CommandText = sQUERY;  //"INSERT INTO BULKINSERTTEST (ID, NAME, ADDRESS) VALUES (:1, :2, :3)";
                    //cmd.ArrayBindCount = ids.Length;
                    //cmd.Parameters.Add(id);
                    //cmd.Parameters.Add(name);
                    //cmd.Parameters.Add(address);
                    cmd.ExecuteNonQuery();
                }



            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

        }

        static void fn_OracleConexion3(
            TransferenciaParametro3 pTransferenciaParametro
            )
        {
            try
            {


                string sQUERY = "";
                DataTable dt = new DataTable();

                sQUERY = "INSERT INTO " + sEsquema + "TLV_HISTORICO  (" +
                    "PERIODO," +
"FECHAPROCESO_VENTA," +
"FECHAACTIVACION," +
"RAZONSOCIAL," +
"C_CONTRATO," +
"TELEFONO," +
"JER_SOCIODENEGOCIO," +
"DESACTIVADO," +
"CANAL," +
"SOCIO," +
"PUNTO_VENTA," +
"VENDEDOR," +
"CLUSTERS," +
"LIMA_REGION," +
"PLAN," +
"RENTA_IGV," +
"TIPO_DOC_MERCADO," +
"MODO_PAGO," +
"TIPO_VENTA," +
"MODO_ORIGEN," +
"PACK_SIM," +
"TECNOLOGIA," +
"TECNOLOGIA2," +
"LLAA," +
"EASY_PACK," +
"PP29_FLEXIBLE," +
"AUTOACTIVADO_AA," +
"METRICA," +
"CAJA_ESQUEMA," +
"MONTO_REVERSION" +
") VALUES (" +
"'" + pTransferenciaParametro.PERIODO + "'," +
"'" + pTransferenciaParametro.FECHAPROCESOVENTA + "'," +
"'" + pTransferenciaParametro.FECHAACTIVACION + "'," +
"'" + pTransferenciaParametro.RAZONSOCIAL + "'," +
"'" + pTransferenciaParametro.C_CONTRATO + "'," +
"'" + pTransferenciaParametro.TELEFONO + "'," +
"'" + pTransferenciaParametro.JER_SOCIODENEGOCIO + "'," +
"'" + pTransferenciaParametro.DESACTIVADO + "'," +
"'" + pTransferenciaParametro.CANAL + "'," +
"'" + pTransferenciaParametro.SOCIO + "'," +
"'" + pTransferenciaParametro.PUNTODEVENTA + "'," +
"'" + pTransferenciaParametro.VENDEDOR + "'," +
"'" + pTransferenciaParametro.CLUSTER + "'," +
"'" + pTransferenciaParametro.LIMAREGION + "'," +
"'" + pTransferenciaParametro.PLAN + "'," +
"'" + pTransferenciaParametro.RENTAIGV + "'," +
"'" + pTransferenciaParametro.TIPODOCUMENTOMERCADO + "'," +
"'" + pTransferenciaParametro.MODOPAGO + "'," +
"'" + pTransferenciaParametro.TIPODEVENTA + "'," +
"'" + pTransferenciaParametro.MODOORIGEN + "'," +
"'" + pTransferenciaParametro.PACKSIM + "'," +
"'" + pTransferenciaParametro.TECNOLOGIA + "'," +
"'" + pTransferenciaParametro.TECNOLOGIA2 + "'," +
"'" + pTransferenciaParametro.LLAA + "'," +
"'" + pTransferenciaParametro.EASYPACK + "'," +
"'" + pTransferenciaParametro.PP29_FLEXIBLE + "'," +
"'" + pTransferenciaParametro.AUTOACTIVADOAA + "'," +
"'" + pTransferenciaParametro.METRICA + "'," +
"'" + pTransferenciaParametro.CAJAESQUEMA + "'," +
"'" + pTransferenciaParametro.MONTOREVERSION + "'" + ")";

                using (var con = new Oracle.ManagedDataAccess.Client.OracleConnection(sConexion))
                {
                    con.Open();
                    int[] foos = new int[3] { 1, 2, 3 };
                    string[] bars = new string[3] { "A", "B", "C" };


                    OracleParameter id = new OracleParameter();
                    id.OracleDbType = OracleDbType.Varchar2;
                    id.Value = pTransferenciaParametro.PERIODO;
                    /*
                    OracleParameter name = new OracleParameter();
                    name.OracleDbType = OracleDbType.Varchar2;
                    name.Value = names;

                    OracleParameter address = new OracleParameter();
                    address.OracleDbType = OracleDbType.Varchar2;
                    address.Value = addresses;
                    */
                    // create command and set properties
                    OracleCommand cmd = con.CreateCommand();
                    cmd.CommandText = sQUERY;  //"INSERT INTO BULKINSERTTEST (ID, NAME, ADDRESS) VALUES (:1, :2, :3)";
                    //cmd.ArrayBindCount = ids.Length;
                    //cmd.Parameters.Add(id);
                    //cmd.Parameters.Add(name);
                    //cmd.Parameters.Add(address);
                    cmd.ExecuteNonQuery();
                }



            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        static void fn_EjecutarReversiones(string pPeriodo)
        {
            string sQUERY = "";

            sQUERY = "INSERT INTO " + sEsquema + "\"CI_TMP_REVERSIONES\"" +
                     "  SELECT A.PERIODOGROSS PERIODO_ACTIVACION, " +
                     "   TO_CHAR(A.FECHAACTIVACION, 'YYYYMMDD') FECHA_ACTIVACION, " +
       "A.PERIODO PERIODO_DESACTIVACION," +
        "TO_CHAR(A.FECHAPROCESO, 'YYYYMMDD') FECHA_DESACTIVACION," +
        "A.C_CONTRATO, " +
        "A.DOCUMENTO, " +
        "A.RAZONSOCIAL NOMBRE," +
        "B.PERIODO PERIODO_VENTA," +
        "TO_CHAR(TO_DATE(B.FECHAPROCESO, 'DD/MM/YYYY'), 'YYYYMMDD') FECHA_VENTA," +
        "B.RENTABASICA RENTA_BASICA," +
        "B.INCENTIVO_SSGG_MANDATO + B.INCENTIVO_JJVV_MANDATO + B.INCENTIVO_TOTAL_PROMOTOR + nvl(B.UNITARIO_58, 0) TOTAL_UNITARIO_REVERSION," +
         "'GGSS' CANAL_VENTA," +
         "SYSDATE FECHA_CARGA" +
        "FROM  " + sEsquema + "\"MP_DEACS_ACUM\" A," +
         sEsquema + "\"GGSS_HISTORICO\" B " +
        "WHERE A.FECHAPROCESO - A.FECHAACTIVACION <= 180 " +
     "     AND A.PERIODO = '" + pPeriodo + "' " +
        "  AND A.C_CONTRATO = B.C_CONTRATO; ";

            fn_Registrar(sQUERY);



        }

        static void fn_EjecutarTiendas(string pPeriodo)
        {
            string sQUERY = "";

            sQUERY = "INSERT INTO " + sEsquema + "\"CI_TMP_REVERSIONES\"" +

                "  SELECT  A.PERIODOGROSS PERIODO_ACTIVACION," +
        "TO_CHAR(A.FECHAACTIVACION, 'YYYYMMDD') FECHA_ACTIVACION," +
        "A.PERIODO PERIODO_DESACTIVACION," +
        "TO_CHAR(A.FECHAPROCESO, 'YYYYMMDD') FECHA_DESACTIVACION," +
        "A.C_CONTRATO," +
        "A.DOCUMENTO," +
        "A.RAZONSOCIAL NOMBRE," +
        "TO_CHAR(TO_DATE(B.FECHAPROCESO), 'YYYYMM') PERIODO_VENTA," +
        "TO_CHAR(TO_DATE(B.FECHAPROCESO, 'DD/MM/YYYY'), 'YYYYMMDD') FECHA_VENTA," +
        "B.RENTAIGV RENTA_BASICA," +
        "B.MONTO_COMISION TOTAL_UNITARIO_REVERSION," +
        "'TIENDAS' CANAL_VENTA," +
        "SYSDATE FECHA_CARGA" +

"FROM " + sEsquema + "MP_DEACS_ACUM A," +
        "TDAS_HISTORICO B" +
"WHERE A.FECHAPROCESO - A.FECHAACTIVACION <= 180" +
        "AND A.PERIODO = '" + pPeriodo + "'" +
        "AND A.C_CONTRATO = B.C_CONTRATO;";
            fn_Registrar(sQUERY);



        }

        static void fn_EjecutarTeleventas(string pPeriodo)
        {
            string sQUERY = "";

            sQUERY = "INSERT INTO " + sEsquema + "\"CI_TMP_REVERSIONES\"" +

        "SELECT  A.PERIODOGROSS PERIODO_ACTIVACION," +
        "TO_CHAR(A.FECHAACTIVACION, 'YYYYMMDD') FECHA_ACTIVACION," +
        "A.PERIODO PERIODO_DESACTIVACION," +
        "TO_CHAR(A.FECHAPROCESO, 'YYYYMMDD') FECHA_DESACTIVACION," +
        "A.C_CONTRATO, " +
        "A.DOCUMENTO," +
        "A.RAZONSOCIAL NOMBRE," +
        "TO_CHAR(TO_DATE(B.FECHAPROCESO_VENTA), 'YYYYMM') PERIODO_VENTA," +
        "TO_CHAR(TO_DATE(B.FECHAPROCESO_VENTA, 'DD/MM/YYYY'), 'YYYYMMDD') FECHA_VENTA," +
        "B.RENTA_IGV RENTA_BASICA," +
        "B.MONTO_REVERSION TOTAL_UNITARIO_REVERSION," +
        "'TELEVENTAS' CANAL_VENTA," +
        "SYSDATE FECHA_CARGA" +

"FROM " + sEsquema + "MP_DEACS_ACUM A,"
         + sEsquema + "TLV_HISTORICO B" +
"WHERE A.FECHAPROCESO - A.FECHAACTIVACION <= 180" +
        "AND A.PERIODO = '" + pPeriodo + "'" +
        "AND A.C_CONTRATO = B.C_CONTRATO;";

            fn_Registrar(sQUERY);

        }

        static void fn_OracleConexion(
            TransferenciaParametro pTransferenciaParametro
            )
        {
            try
            {


                string sQUERY = "";
                DataTable dt = new DataTable();

                sQUERY = "INSERT INTO " + sEsquema + "GGSS_HISTORICO  (" +
                    "PERIODO                  , " +
"FECHAPROCESO             , " +
"FECHAACTIVACION           , " +
"RAZONSOCIAL                , " +
"C_CONTRATO                           , " +
"TELEFONO                             , " +
"MODELOEQUIPO                         , " +
"N_PLAN                               , " +
"VENDEDOR                             , " +
"TIPODOCUMENTO                        , " +
"DOCUMENTO                            , " +
"NRO_ORDEN                           , " +
"RENTABASICA                         , " +
"VENDEDOR_PACKSIM                    , " +
"PORTA_CEDENTE                       , " +
"SISTEMAFUENTE                       , " +
"LLAA_BASE_CAPTURA                   , " +
"C_CONTRATOFS                        , " +
"CODIGOBSCS                          , " +
"VENDEDORDNI                         , " +
"FLAG_PRODUCTO                       , " +
"FLAGT0                              , " +
"FLAG_FT                             , " +
"DESACTIVADOS                        , " +
"CANAL                               , " +
"SOCIO                               , " +
"PUNTO_DE_VENTA_NORMALIZADO          , " +
"RENTA_IGV                           , " +
"TIPO_DOCUMENTO_MERCADO              , " +
"MODO_PAGO                           , " +
"TIPO_DE_VENTA                       , " +
"MODO_ORIGEN                         , " +
"PACK_SIM                            , " +
"TECNOLOGIA                          , " +
"TECNOLOGIA50                        , " +
"LLAA                                , " +
"EASY_PACK                           , " +
"PP50_FLEXIBLE                       , " +
"AUTOACTIVADO_AA                     , " +
"METRICA                             , " +
"CONCATENAR                          , " +
"SUB_CAJAS_ESQUEMA_GGSS_V50          , " +
"CAJAS_ESQUEMA_GGSS                  , " +
"CONSIDERA_CAJAS_ESQUEMA_SSNN        , " +
"CAJA_NUEVO_HISTORICO                , " +
"CLUSTERS                            , " +
"CONIH_SINIH                         , " +
"VEP_NOVEP                           , " +
"SUPERVISOR_SSGG                     , " +
"JEFE_DE_VENTAS_JV                   , " +
"INCENTIVO_SSGG_MANDATO              , " +
"INCENTIVO_JJVV_MANDATO              , " +
"INCENTIVO_TOTAL_PROMOTOR            , " +
"UNITARIO_58) VALUES (" +
"'" + pTransferenciaParametro.PERIODO + "'," +
"'" + pTransferenciaParametro.FECHAPROCESO + "'," +
"'" + pTransferenciaParametro.FECHAACTIVACION + "'," +
"'" + pTransferenciaParametro.RAZONSOCIAL + "'," +
"'" + pTransferenciaParametro.C_CONTRATO + "'," +
"'" + pTransferenciaParametro.TELEFONO + "'," +
"'" + pTransferenciaParametro.MODELOEQUIPO + "'," +
"'" + pTransferenciaParametro.N_PLAN + "'," +
"'" + pTransferenciaParametro.VENDEDOR + "'," +
"'" + pTransferenciaParametro.TIPODOCUMENTO + "'," +
"'" + pTransferenciaParametro.DOCUMENTO + "'," +
"'" + pTransferenciaParametro.NRO_ORDEN + "'," +
"'" + pTransferenciaParametro.RENTABASICA + "'," +
"'" + pTransferenciaParametro.VENDEDOR_PACKSIM + "'," +
"'" + pTransferenciaParametro.PORTA_CEDENTE + "'," +
"'" + pTransferenciaParametro.SISTEMAFUENTE + "'," +
"'" + pTransferenciaParametro.LLAA_BASE_CAPTURA + "'," +
"'" + pTransferenciaParametro.C_CONTRATOFS + "'," +
"'" + pTransferenciaParametro.CODIGOBSCS + "'," +
"'" + pTransferenciaParametro.VENDEDORDNI + "'," +
"'" + pTransferenciaParametro.FLAG_PRODUCTO + "'," +
"'" + pTransferenciaParametro.FLAGT0 + "'," +
"'" + pTransferenciaParametro.FLAG_FT + "'," +
"'" + pTransferenciaParametro.DESACTIVADOS + "'," +
"'" + pTransferenciaParametro.CANAL + "'," +
"'" + pTransferenciaParametro.SOCIO + "'," +
"'" + pTransferenciaParametro.PUNTO_DE_VENTA_NORMALIZADO + "'," +
"'" + pTransferenciaParametro.RENTA_IGV + "'," +
"'" + pTransferenciaParametro.TIPO_DOCUMENTO_MERCADO + "'," +
"'" + pTransferenciaParametro.MODO_PAGO + "'," +
"'" + pTransferenciaParametro.TIPO_DE_VENTA + "'," +
"'" + pTransferenciaParametro.MODO_ORIGEN + "'," +
"'" + pTransferenciaParametro.PACK_SIM + "'," +
"'" + pTransferenciaParametro.TECNOLOGIA + "'," +
"'" + pTransferenciaParametro.TECNOLOGIA50 + "'," +
"'" + pTransferenciaParametro.LLAA + "'," +
"'" + pTransferenciaParametro.EASY_PACK + "'," +
"'" + pTransferenciaParametro.PP50_FLEXIBLE + "'," +
"'" + pTransferenciaParametro.AUTOACTIVADO_AA + "'," +
"'" + pTransferenciaParametro.METRICA + "'," +
"'" + pTransferenciaParametro.CONCATENAR + "'," +
"'" + pTransferenciaParametro.SUB_CAJAS_ESQUEMA_GGSS_V50 + "'," +
"'" + pTransferenciaParametro.CAJAS_ESQUEMA_GGSS + "'," +
"'" + pTransferenciaParametro.CONSIDERA_CAJAS_ESQUEMA_SSNN + "'," +
"'" + pTransferenciaParametro.CAJA_NUEVO_HISTORICO + "'," +
"'" + pTransferenciaParametro.CLUSTERS + "'," +
"'" + pTransferenciaParametro.CONIH_SINIH + "'," +
"'" + pTransferenciaParametro.VEP_NOVEP + "'," +
"'" + pTransferenciaParametro.SUPERVISOR_SSGG + "'," +
"'" + pTransferenciaParametro.JEFE_DE_VENTAS_JV + "'," +
"'" + pTransferenciaParametro.INCENTIVO_SSGG_MANDATO + "'," +
"'" + pTransferenciaParametro.INCENTIVO_JJVV_MANDATO + "'," +
"'" + pTransferenciaParametro.INCENTIVO_TOTAL_PROMOTOR + "'," +
"'" + pTransferenciaParametro.UNITARIO_58 + "'" + ")";

                using (var con = new Oracle.ManagedDataAccess.Client.OracleConnection(sConexion))
                {
                    con.Open();
                    int[] foos = new int[3] { 1, 2, 3 };
                    string[] bars = new string[3] { "A", "B", "C" };


                    OracleParameter id = new OracleParameter();
                    id.OracleDbType = OracleDbType.Varchar2;
                    id.Value = pTransferenciaParametro.PERIODO;
                    /*
                    OracleParameter name = new OracleParameter();
                    name.OracleDbType = OracleDbType.Varchar2;
                    name.Value = names;

                    OracleParameter address = new OracleParameter();
                    address.OracleDbType = OracleDbType.Varchar2;
                    address.Value = addresses;
                    */
                    // create command and set properties
                    OracleCommand cmd = con.CreateCommand();
                    cmd.CommandText = sQUERY;  //"INSERT INTO BULKINSERTTEST (ID, NAME, ADDRESS) VALUES (:1, :2, :3)";
                    //cmd.ArrayBindCount = ids.Length;
                    //cmd.Parameters.Add(id);
                    //cmd.Parameters.Add(name);
                    //cmd.Parameters.Add(address);
                    cmd.ExecuteNonQuery();
                }



            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

        }

        static Archivos fn_VaciaExcel(string pRutaBaseArchivo, string pHoja, string pID, string pPeriodo, string pTOPREGISTROS, string pIDRegistro, string pNombreArchivo)
        {
            Archivos oArchivo = new Archivos();
            string sAño = "";
            string sMes = "";
            string sMesText = "";

            try
            {

                string sBase = pRutaBaseArchivo; //System.Configuration.ConfigurationManager.AppSettings["param1"] ;
                string sHoja = pHoja;//System.Configuration.ConfigurationManager.AppSettings["param2"] ;

                //string sBase =  System.Configuration.ConfigurationManager.AppSettings["param1"] ;
                //string sHoja = System.Configuration.ConfigurationManager.AppSettings["param2"] ;

                int xValor = 1;
                //public static string connStr = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + path + ";Extended Properties=excel 12.0;";

                //Fuente: https://www.iteramos.com/pregunta/9358/excel-quotla-tabla-externa-no-tiene-el-formato-esperadoquot
                System.Data.DataSet DtSet;
                DtSet = new System.Data.DataSet();

                Console.WriteLine("********Conectandose a archivo Excel************************" + pRutaBaseArchivo);

                Console.WriteLine("********Inicio ********" + DateTime.Now);
                //Console.Read();
                
                //using (System.Data.OleDb.OleDbConnection oOleDbConnection = new System.Data.OleDb.OleDbConnection("Provider=Microsoft.ACE.OLEDB.12.0;Data Source='" + sBase + "';Extended Properties=excel 12.0;"))
                using (System.Data.OleDb.OleDbConnection oOleDbConnection = new System.Data.OleDb.OleDbConnection("Provider = Microsoft.ACE.OLEDB.12.0; Data Source='" + sBase + "';Extended Properties = \"Excel 12.0 Xml; HDR = YES\";"))
                {
                    oOleDbConnection.Open();
                    string C_CONTRATO_FILTRO = "";
                    string sQueryExcel = ""; //"select * from " + sHoja + " where PERIODO ='202201'";

                    if (pID == "4")
                    {
                        sAño = pPeriodo.Substring(0, 4);
                        sMes = pPeriodo.Substring(sAño.Length, 2);
                        sMesText = "";

                        switch (sMes)
                        {
                            case "01": sMesText = "ENERO"; break;
                            case "02": sMesText = "FEBRERO"; break;
                            case "03": sMesText = "MARZO"; break;
                            case "04": sMesText = "ABRIL"; break;
                            case "05": sMesText = "MAYO"; break;
                            case "06": sMesText = "JUNIO"; break;
                            case "07": sMesText = "JULIO"; break;
                            case "08": sMesText = "AGOSTO"; break;
                            case "09": sMesText = "SEPTIEMBRE"; break;
                            case "10": sMesText = "OCTUBRE"; break;
                            case "11": sMesText = "NOVIEMBRE"; break;
                            case "12": sMesText = "DICIEMBRE"; break;
                            default:
                                sMesText = "99";
                                break;
                        }

                        // aplicar limit 
                        string sQUERY = "SELECT C_CONTRATO FROM " + sEsquema + "\"TDAS_HISTORICO\" WHERE  AÑO  ='" + sAño + "' and  MES = '" + sMesText + "' AND ROWNUM < " + pTOPREGISTROS.Replace("top","")  ;

                        DataTable oObjArchivos_HISTORICOS = new DataTable();
                        using (OracleDataAdapter adp = new OracleDataAdapter(sQUERY, sConexion))
                        {
                            adp.Fill(oObjArchivos_HISTORICOS);//all the data in OracleAdapter will be filled into Datatable 
                        }
                        C_CONTRATO_FILTRO = "";
                        int xc = 1; int xc2 = oObjArchivos_HISTORICOS.Rows.Count;
                        foreach (DataRow oRows in oObjArchivos_HISTORICOS.Rows)
                        {
                            if (xc == xc2)
                                C_CONTRATO_FILTRO += "'" + oRows["C_CONTRATO"].ToString() + "'";
                            else
                                C_CONTRATO_FILTRO += "'" + oRows["C_CONTRATO"].ToString() + "',";
                            xc++;
                        }

                        if (C_CONTRATO_FILTRO.Length > 0)
                        {
                            // AQUI es filtrpo
                            //C_CONTRATO_FILTRO = "'-115454762'";
                            sQueryExcel = "select " + pTOPREGISTROS + " * from " + sHoja + " where [AÑO] =" + sAño + " and [MES]= '" + sMesText + "' and [C_CONTRATO] NOT IN(" + C_CONTRATO_FILTRO + ")";
                            //sQueryExcel = "select " + pTOPREGISTROS + " * from " + sHoja + " where [AÑO] =" + sAño + " and [MES]= '" + sMesText + "'";
                        }
                        else
                        {
                            sQueryExcel = "select " + pTOPREGISTROS + " * from " + sHoja + " where [AÑO] =" + sAño + " and [MES]= '" + sMesText + "'";
                        }
                    }
                    else
                    {
                        DataTable oObjArchivos_HISTORICOS = new DataTable();

                        string sQUERY = "";

                        if (pID == "1")
                        {
                            oObjArchivos_HISTORICOS = new DataTable();
                            sQUERY = "SELECT C_CONTRATO FROM " + sEsquema + "\"GGSS_HISTORICO\" WHERE PERIODO ='" + pPeriodo + "' AND ROWNUM < " + pTOPREGISTROS.Replace("top", "") ;
                            Console.WriteLine("sQUERY " + sQUERY);
                            using (OracleDataAdapter adp = new OracleDataAdapter(sQUERY, sConexion))
                            {
                                adp.Fill(oObjArchivos_HISTORICOS);//all the data in OracleAdapter will be filled into Datatable 
                            }
                            int xc = 1; int xc2 = oObjArchivos_HISTORICOS.Rows.Count;
                            foreach (DataRow oRows in oObjArchivos_HISTORICOS.Rows)
                            {
                                if (xc == xc2)
                                    C_CONTRATO_FILTRO += "'" + oRows["C_CONTRATO"].ToString() + "'";
                                else
                                    C_CONTRATO_FILTRO += "'" + oRows["C_CONTRATO"].ToString() + "',";
                                xc++;
                            }
                        }
                        else if (pID == "3")
                        {
                            oObjArchivos_HISTORICOS = new DataTable();
                            sQUERY = "SELECT C_CONTRATO FROM " + sEsquema + "\"TLV_HISTORICO\" WHERE PERIODO ='" + pPeriodo  + "' AND ROWNUM < " + pTOPREGISTROS.Replace("top", "") ;
                            using (OracleDataAdapter adp = new OracleDataAdapter(sQUERY, sConexion))
                            {
                                adp.Fill(oObjArchivos_HISTORICOS);//all the data in OracleAdapter will be filled into Datatable 
                            }
                            int xc = 1; int xc2 = oObjArchivos_HISTORICOS.Rows.Count;


                            foreach (DataRow oRows in oObjArchivos_HISTORICOS.Rows)
                            {
                                if (xc == xc2)
                                    C_CONTRATO_FILTRO += "'" + oRows["C_CONTRATO"].ToString() + "'";
                                else
                                    C_CONTRATO_FILTRO += "'" + oRows["C_CONTRATO"].ToString() + "',";
                                xc++;
                            }

                            

                        }
                        else if (pID == "4")
                        {

                        }
                        Console.WriteLine("sQueryE " + sQUERY);

                    }
                    if (pID == "4")
                    {

                    }
                    else
                    {
                        if (C_CONTRATO_FILTRO.Length > 0)
                        {
                            //sQueryExcel = "select " + pTOPREGISTROS + " * from " + sHoja + " where PERIODO ='" + pPeriodo + "' and C_CONTRATO NOT IN(" + C_CONTRATO_FILTRO + ")";
                            //C_CONTRATO_FILTRO = "-115454762";
                            sQueryExcel = "select " + pTOPREGISTROS + " * from " + sHoja + " where PERIODO ='" + pPeriodo + "' and [C_CONTRATO] NOT IN(" + C_CONTRATO_FILTRO + ")";

                        }
                        else
                        {
                            sQueryExcel = "select  " + pTOPREGISTROS + " * from " + sHoja + " where PERIODO ='" + pPeriodo + "'";
                        }
                    }

                    

                    Console.WriteLine("pID " + pID);
                    Console.WriteLine("sQueryExcel " + sQueryExcel);

                    using (System.Data.OleDb.OleDbDataAdapter oOleDbDataAdapter =
                        new System.Data.OleDb.OleDbDataAdapter(sQueryExcel, oOleDbConnection))
                    {

                        Console.WriteLine("Obteniendo registros de la hoja  " + sHoja + "............");
                        //  Console.Read();

                        oOleDbDataAdapter.TableMappings.Add("Table", "TestTable");
                        DtSet = new System.Data.DataSet();
                        oOleDbDataAdapter.Fill(DtSet);

                        DataTable firstTable = DtSet.Tables[0];
                        Console.WriteLine("Total Registros obtenidos " + firstTable.Rows.Count);

                        //return null;
                        string sQueryUpdate = "update " + sEsquema + "\"MIGRACIONTRANSFERENCIA\" set TOTALREGISTROSORIGEN='" + firstTable.Rows.Count + "' WHERE IDENTI ='" + pIDRegistro + "'";

                        Console.WriteLine("sQueryUpdate_rO " + sQueryUpdate);

                        //fn_Registrar(sQueryUpdate);
                        //  Console.Read();
                        Console.WriteLine("sQueryUpdate " + sQueryUpdate);

                        xValor = 1;

                        int cContador = 1;
                        // AQUI MCOX

                        //SaveUsingOracleBulkCopy(firstTable);
                        //return null;

                        foreach (DataRow oRows in firstTable.Rows)

                        //DataTable oObjP = new DataTable();
                        //foreach (DataRow oRows in oObjP.Rows)
                        {

                            if (pID == "1")
                            {
                                string sQUERY_VALIDACION = "SELECT C_CONTRATO FROM " + sEsquema + "\"GGSS_HISTORICO\"  WHERE C_CONTRATO='" + oRows["C_CONTRATO"] + "'";
                                 
                                Console.WriteLine("sQUERY_VALIDACION 1" + sQUERY_VALIDACION);

                                DataTable oObjArchivos_HISTORICOS_VALIDACION_3 = new DataTable();
                                using (OracleDataAdapter adp = new OracleDataAdapter(sQUERY_VALIDACION, sConexion))
                                {
                                    adp.Fill(oObjArchivos_HISTORICOS_VALIDACION_3);//all the data in OracleAdapter will be filled into Datatable 
                                }

                                Console.WriteLine("oObjArchivos_HISTORICOS_VALIDACION.Rows.Count " + oObjArchivos_HISTORICOS_VALIDACION_3.Rows.Count);

                                if (oObjArchivos_HISTORICOS_VALIDACION_3.Rows.Count == 0)
                                {
                                    fn_SubirArchivo1(oRows);
                                }
                                
                            }
                            if (pID == "3")
                            {
                                
                                string sQUERY_VALIDACION  = "SELECT C_CONTRATO FROM " + sEsquema + "\"TLV_HISTORICO\"  WHERE C_CONTRATO='" + oRows["C_CONTRATO"] + "'";

                                Console.WriteLine("sQUERY_VALIDACION 3" + sQUERY_VALIDACION);

                                DataTable oObjArchivos_HISTORICOS_VALIDACION_3 = new DataTable();
                                using (OracleDataAdapter adp = new OracleDataAdapter(sQUERY_VALIDACION, sConexion))
                                {
                                    adp.Fill(oObjArchivos_HISTORICOS_VALIDACION_3);//all the data in OracleAdapter will be filled into Datatable 
                                }

                                Console.WriteLine("oObjArchivos_HISTORICOS_VALIDACION.Rows.Count " + oObjArchivos_HISTORICOS_VALIDACION_3.Rows.Count);

                                if (oObjArchivos_HISTORICOS_VALIDACION_3.Rows.Count == 0)
                                {
                                    fn_SubirArchivo3(oRows);
                                }
                                /*
                                else
                                {

                                    string sQueryUpdate2 = "update " + sEsquema + "\"MIGRACIONTRANSFERENCIA\" set CANTIDADMIGRACION='" + cContador + "' WHERE IDENTI ='" + pIDRegistro + "'";
                                    fn_Registrar(sQueryUpdate2);

                                    Console.WriteLine("sQueryUpdate " + sQueryUpdate2);
                                    cContador++;
                                }
                                */

                                //fn_SubirArchivo3(oRows);
                            }
                            else if (pID == "4")
                            {
                                string sQUERY_VALIDACION = "SELECT C_CONTRATO FROM " + sEsquema + "\"TDAS_HISTORICO\" WHERE C_CONTRATO='" + oRows["C_CONTRATO"] + "'";

                                Console.WriteLine("sQUERY_VALIDACION 4" + sQUERY_VALIDACION);

                                DataTable oObjArchivos_HISTORICOS_VALIDACION_2 = new DataTable();
                                using (OracleDataAdapter adp = new OracleDataAdapter(sQUERY_VALIDACION, sConexion))
                                {
                                    adp.Fill(oObjArchivos_HISTORICOS_VALIDACION_2);//all the data in OracleAdapter will be filled into Datatable 
                                }

                                Console.WriteLine("oObjArchivos_HISTORICOS_VALIDACION.Rows.Count " + oObjArchivos_HISTORICOS_VALIDACION_2.Rows.Count);

                                if (oObjArchivos_HISTORICOS_VALIDACION_2.Rows.Count == 0)
                                {
                                    fn_SubirArchivo4(oRows);
                                }
                                /*
                                else
                                {

                                    string sQueryUpdate2 = "update " + sEsquema + "\"MIGRACIONTRANSFERENCIA\" set CANTIDADMIGRACION='" + cContador + "' WHERE IDENTI ='" + pIDRegistro + "'";
                                    //fn_Registrar(sQueryUpdate2);

                                    Console.WriteLine("sQueryUpdate " + sQueryUpdate2);
                                    cContador++;
                                }
                                */
                            }
                            else
                            {
                                string sQueryUpdate2 = "update " + sEsquema + "\"MIGRACIONTRANSFERENCIA\" set CANTIDADMIGRACION='" + cContador + "' WHERE IDENTI ='" + pIDRegistro + "'";
                                //fn_Registrar(sQueryUpdate2);

                                Console.WriteLine("sQueryUpdate " + sQueryUpdate2);
                                cContador++;

                            }

                        }

                        string sQueryUpdateTermino = "update " + sEsquema + "\"MIGRACIONTRANSFERENCIA\" set ESTADO='" + "TERMINADO" + "', HORAFINAL='" + DateTime.Now.ToShortTimeString() + "' WHERE IDENTI ='" + pIDRegistro + "'";
                        Console.WriteLine("sQueryUpdateTermino " + sQueryUpdateTermino);

                        fn_Registrar(sQueryUpdateTermino);

                        //ZZZ 

                        /*  EJECUTAMOS    */

                        if (pID == "1")
                        {
                            //fn_EjecutarReversiones(pPeriodo);
                        }

                        else if (pID == "3")
                        {
                            //fn_EjecutarTiendas(pPeriodo);
                        }
                        else if (pID == "4")
                        {
                            //fn_EjecutarTeleventas(pPeriodo);
                        }


                        oArchivo.pNombreArchivo = pNombreArchivo;
                        oArchivo.pPeriodo = pPeriodo;
                        oArchivo.pTotalRegistro = firstTable.Rows.Count.ToString();
                        oArchivo.pRegistrosCopiado = cContador.ToString();
                        oArchivo.pFechaUtilmaCarga = DateTime.Now.ToShortDateString();



                        //  Console.Read();
                        Console.WriteLine("sQueryUpdateTermino_" + sQueryUpdateTermino);

                        Console.WriteLine("Termino Proceso ..." + DateTime.Now);
                        //Thread.Sleep(4000);
                    }

                    Console.WriteLine("Finalizando ....");
                    //Thread.Sleep(4000);
                    // Console.Read();

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.Read();
            }
            return oArchivo;
        }

        static void fn_EjecutarMacro()
        {
            Process scriptProc = new Process();
            scriptProc.StartInfo.FileName = @"cscript";
            scriptProc.StartInfo.WorkingDirectory = @"C:\Users\prod_iherediav\Desktop\"; //<---very important 
            scriptProc.StartInfo.Arguments = "//B //Nologo Correo_Oracle_1.vbs";
            scriptProc.StartInfo.WindowStyle = ProcessWindowStyle.Hidden; //prevent console window from popping up
            scriptProc.Start();
            scriptProc.WaitForExit(); // <-- Optional if you want program running until your script exit
            scriptProc.Close();

        }
        static void fn_SubirArchivo1(DataRow oRows)
        {
            //Console.WriteLine("Migrado :" + cContador + " / " + oRows["C_CONTRATO"].ToString());
            TransferenciaParametro pTransferenciaParametro = new TransferenciaParametro();
            pTransferenciaParametro.PERIODO = oRows["PERIODO"].ToString(); //0

            //oRows["FECHAPROCESO VENTA"].ToString();
            pTransferenciaParametro.FECHAPROCESO = oRows["FECHAPROCESO"].ToString(); //1
            pTransferenciaParametro.FECHAACTIVACION = oRows["FECHAACTIVACION"].ToString();//2
            pTransferenciaParametro.RAZONSOCIAL = oRows["RAZONSOCIAL"].ToString();//3
            pTransferenciaParametro.C_CONTRATO = oRows["C_CONTRATO"].ToString(); //4
            pTransferenciaParametro.TELEFONO = oRows["TELEFONO"].ToString();//5
            pTransferenciaParametro.MODELOEQUIPO = oRows["MODELOEQUIPO"].ToString();//6
            pTransferenciaParametro.N_PLAN = oRows["N_PLAN"].ToString();//7
            pTransferenciaParametro.VENDEDOR = oRows["VENDEDOR"].ToString();//8
            pTransferenciaParametro.TIPODOCUMENTO = oRows["TIPODOCUMENTO"].ToString();//9
            pTransferenciaParametro.DOCUMENTO = oRows["DOCUMENTO"].ToString();//10
            pTransferenciaParametro.NRO_ORDEN = oRows["NRO_ORDEN"].ToString();//11
            pTransferenciaParametro.RENTABASICA = oRows["RENTABASICA"].ToString(); // 12
            pTransferenciaParametro.VENDEDOR_PACKSIM = oRows["VENDEDOR_PACKSIM"].ToString(); //13
            pTransferenciaParametro.PORTA_CEDENTE = oRows["PORTA_CEDENTE"].ToString(); //14
            pTransferenciaParametro.SISTEMAFUENTE = oRows["SISTEMAFUENTE"].ToString(); //15
            pTransferenciaParametro.LLAA_BASE_CAPTURA = oRows["LLAA_BASE_CAPTURA"].ToString(); //16
            pTransferenciaParametro.C_CONTRATOFS = oRows["C_CONTRATOFS"].ToString(); //17
            pTransferenciaParametro.CODIGOBSCS = oRows["CODIGOBSCS"].ToString(); //18
            pTransferenciaParametro.VENDEDORDNI = oRows["VendedorDNI"].ToString(); //19
            pTransferenciaParametro.FLAG_PRODUCTO = oRows["FLAG_Producto"].ToString(); //20
            pTransferenciaParametro.FLAGT0 = oRows["FLAGT0"].ToString(); //21
            pTransferenciaParametro.FLAG_FT = oRows["FLAG_FT"].ToString(); //22
            pTransferenciaParametro.DESACTIVADOS = oRows["DESACTIVADOS"].ToString(); //23
            pTransferenciaParametro.CANAL = oRows["CANAL"].ToString();//24
            pTransferenciaParametro.SOCIO = oRows["SOCIO"].ToString();//25
            pTransferenciaParametro.PUNTO_DE_VENTA_NORMALIZADO = oRows["PUNTO DE VENTA NORMALIZADO"].ToString(); //26                                
            pTransferenciaParametro.RENTA_IGV = oRows["RENTA IGV"].ToString(); //27
            pTransferenciaParametro.TIPO_DOCUMENTO_MERCADO = oRows["TIPO DOCUMENTO MERCADO"].ToString();//28
            pTransferenciaParametro.MODO_PAGO = oRows["MODO PAGO"].ToString(); //29
            pTransferenciaParametro.TIPO_DE_VENTA = oRows["TIPO DE VENTA"].ToString(); //30
            pTransferenciaParametro.MODO_ORIGEN = oRows["MODO ORIGEN"].ToString();//31
            pTransferenciaParametro.PACK_SIM = oRows["PACK / SIM"].ToString(); //32
            pTransferenciaParametro.TECNOLOGIA = oRows["TECNOLOGIA"].ToString(); //33
            pTransferenciaParametro.TECNOLOGIA50 = oRows["TECNOLOGIA2"].ToString(); //34
            pTransferenciaParametro.LLAA = oRows["LLAA"].ToString();//35
            pTransferenciaParametro.EASY_PACK = oRows["EASY PACK"].ToString();//36
            pTransferenciaParametro.PP50_FLEXIBLE = oRows["PP29_FLEXIBLE"].ToString();//37 
            pTransferenciaParametro.AUTOACTIVADO_AA = oRows["AUTOACTIVADO AA"].ToString(); //38
            pTransferenciaParametro.METRICA = oRows["METRICA"].ToString(); //39
            pTransferenciaParametro.CONCATENAR = oRows["CONCATENAR"].ToString();//40
            pTransferenciaParametro.SUB_CAJAS_ESQUEMA_GGSS_V50 = oRows["SUB_CAJAS ESQUEMA GGSS V2"].ToString();//41
            pTransferenciaParametro.CAJAS_ESQUEMA_GGSS = oRows["CAJAS ESQUEMA GGSS"].ToString();//42
                                                                                                //oRows["CAJA ESQUEMA"].ToString();
            pTransferenciaParametro.CONSIDERA_CAJAS_ESQUEMA_SSNN = oRows["CONSIDERA CAJAS ESQUEMA SSNN"].ToString(); //43
            pTransferenciaParametro.CAJA_NUEVO_HISTORICO = oRows["CAJA NUEVO HISTORICO"].ToString(); //44 AQUI VACIO TELEVENTAS                                                                           //""; "", "", "" ,"", "", "", "", "",""
            pTransferenciaParametro.CLUSTERS = oRows["CLUSTER"].ToString(); //45
            pTransferenciaParametro.CONIH_SINIH = oRows["CON IH/ SIN IH"].ToString();
            pTransferenciaParametro.VEP_NOVEP = oRows["VEP / NO VEP"].ToString();
            pTransferenciaParametro.SUPERVISOR_SSGG = oRows["SUPERVISOR SSGG"].ToString();
            pTransferenciaParametro.JEFE_DE_VENTAS_JV = oRows["JEFE DE VENTAS JV"].ToString();
            pTransferenciaParametro.INCENTIVO_SSGG_MANDATO = oRows["INCENTIVO SSGG MANDATO"].ToString();
            pTransferenciaParametro.INCENTIVO_JJVV_MANDATO = oRows["INCENTIVO JJVV MANDATO"].ToString();
            pTransferenciaParametro.INCENTIVO_TOTAL_PROMOTOR = oRows["INCENTIVO TOTAL PROMOTOR"].ToString();
            pTransferenciaParametro.UNITARIO_58 = oRows["UNITARIO_58"].ToString();
            fn_OracleConexion(pTransferenciaParametro);

        }

        static void fn_SubirArchivo3(DataRow oRows)
        {
            //Console.WriteLine("Migrado :" + cContador + " / " + oRows["C_CONTRATO"].ToString());
            TransferenciaParametro3 pTransferenciaParametro = new TransferenciaParametro3();
            pTransferenciaParametro.PERIODO = oRows["PERIODO"].ToString(); //0
            pTransferenciaParametro.FECHAPROCESOVENTA = oRows["FECHAPROCESO VENTA"].ToString(); //1
            pTransferenciaParametro.FECHAACTIVACION = oRows["FECHAACTIVACION"].ToString();//2
            pTransferenciaParametro.RAZONSOCIAL = oRows["RAZONSOCIAL"].ToString();//3
            pTransferenciaParametro.C_CONTRATO = oRows["C_CONTRATO"].ToString(); //4
            pTransferenciaParametro.TELEFONO = oRows["TELEFONO"].ToString();//5
            pTransferenciaParametro.JER_SOCIODENEGOCIO = oRows["JER_SOCIODENEGOCIO"].ToString();//6
            pTransferenciaParametro.DESACTIVADO = oRows["DESACTIVADO"].ToString();//7
            pTransferenciaParametro.CANAL = oRows["CANAL"].ToString();//8
            pTransferenciaParametro.SOCIO = oRows["SOCIO"].ToString();//9
            pTransferenciaParametro.PUNTODEVENTA = oRows["PUNTO DE VENTA"].ToString();//10  ///
            pTransferenciaParametro.VENDEDOR = oRows["VENDEDOR"].ToString(); // 11
            pTransferenciaParametro.CLUSTER = oRows["CLUSTER"].ToString(); //12
            pTransferenciaParametro.LIMAREGION = oRows["LIMA / REGION"].ToString(); //13
            pTransferenciaParametro.PLAN = oRows["PLAN"].ToString(); //14
            pTransferenciaParametro.RENTAIGV = oRows["RENTA IGV"].ToString(); //15
            pTransferenciaParametro.TIPODOCUMENTOMERCADO = oRows["TIPO DOCUMENTO MERCADO"].ToString(); //16
            pTransferenciaParametro.MODOPAGO = oRows["MODO PAGO"].ToString(); //17
            pTransferenciaParametro.TIPODEVENTA = oRows["TIPO DE VENTA"].ToString(); //18
            pTransferenciaParametro.MODOORIGEN = oRows["MODO ORIGEN"].ToString(); //19
            pTransferenciaParametro.PACKSIM = oRows["PACK / SIM"].ToString(); //20
            pTransferenciaParametro.TECNOLOGIA = oRows["TECNOLOGIA"].ToString(); //21
            pTransferenciaParametro.TECNOLOGIA2 = oRows["TECNOLOGIA2"].ToString(); //22
            pTransferenciaParametro.LLAA = oRows["LLAA"].ToString();//23
            pTransferenciaParametro.EASYPACK = oRows["EASY PACK"].ToString();//24
            pTransferenciaParametro.PP29_FLEXIBLE = oRows["PP29_FLEXIBLE"].ToString(); //25                                
            pTransferenciaParametro.AUTOACTIVADOAA = oRows["AUTOACTIVADO AA"].ToString(); //26
            pTransferenciaParametro.METRICA = oRows["METRICA"].ToString();//27
            pTransferenciaParametro.CAJAESQUEMA = oRows["CAJA ESQUEMA"].ToString(); //28
            pTransferenciaParametro.MONTOREVERSION = oRows["MONTO REVERSION"].ToString(); //29
            fn_OracleConexion3(pTransferenciaParametro);


        }

        static void fn_SubirArchivo4(DataRow oRows)
        {
            //Console.WriteLine("Migrado :" + cContador + " / " + oRows["C_CONTRATO"].ToString());
            TransferenciaParametro4 pTransferenciaParametro = new TransferenciaParametro4();
            pTransferenciaParametro.C_CONTRATO = oRows["C_CONTRATO"].ToString(); //0
            pTransferenciaParametro.FECHAPROCESO = oRows["FECHAPROCESO"].ToString(); //1
            pTransferenciaParametro.N_PLAN = oRows["N_PLAN"].ToString();//2
            pTransferenciaParametro.ESTADOINAR = oRows["ESTADOINAR"].ToString();//3
            pTransferenciaParametro.RENTAIGV = oRows["RENTAIGV"].ToString(); //4
            pTransferenciaParametro.MODOPAGO = oRows["MODOPAGO"].ToString();//5
            pTransferenciaParametro.TIPOVENTA = oRows["TIPOVENTA"].ToString();//6
            pTransferenciaParametro.MODEL_F = oRows["MODEL_F"].ToString();//7
            pTransferenciaParametro.SOCIO = oRows["SOCIO"].ToString();//8
            pTransferenciaParametro.VISTA_METRICA = oRows["VISTA METRICA"].ToString();//9
            pTransferenciaParametro.MONTO_COMISION = oRows["MONTO COMISION"].ToString();//10  ///
            pTransferenciaParametro.MES = oRows["MES"].ToString(); // 11
            pTransferenciaParametro.AÑO = oRows["AÑO"].ToString(); //12 
            fn_OracleConexion4(pTransferenciaParametro);

        }


        static void fn_EnviarCorreo(List<Archivos> oLista)
        {

            Correo oCorreo = new Correo();
            List<CorreoDestinatario> oListaCorreoDestinatario = new List<CorreoDestinatario>();
            CorreoDestinatario oCorreoDestinatario = new CorreoDestinatario();
            oCorreoDestinatario.Descripcion = "eherrera@rcpasesores.com";
            //oCorreoDestinatario.Descripcion = "mcox2908@gmail.com";
            oListaCorreoDestinatario.Add(oCorreoDestinatario);

            oCorreoDestinatario = new CorreoDestinatario();
            oCorreoDestinatario.Descripcion = "wcox@rcpasesores.com";
            oListaCorreoDestinatario.Add(oCorreoDestinatario);

            oCorreoDestinatario = new CorreoDestinatario();
            oCorreoDestinatario.Descripcion = "Igor.heredia@entel.pe";
            oListaCorreoDestinatario.Add(oCorreoDestinatario);

            oCorreoDestinatario = new CorreoDestinatario();
            oCorreoDestinatario.Descripcion = "externo.eherrerac@externo.entel.pe";
            oListaCorreoDestinatario.Add(oCorreoDestinatario);

            string sCuerpo = fn_CargarDetalle(oLista);


            //fn_CargarDetalle()
            oCorreo.fn_EnviarDemonio("wcox@rcpasesores.com", "12345678", "smtp.gmail.com", 25, "wcox@rcpasesores.com",
                "PROCESO CONCLUIDO", "TABLA REVERSIONES ACTUALIZADA AL ", sCuerpo,
                oListaCorreoDestinatario, false, null, null, null, true, false, true);

        }

        private static string fn_CargarDetalle(List<Archivos> pListaArchivos)
        {
            string str = "<table border='1'>" +
                "<tr><td style='width: 100px; height: 21px;'>NOMBRE_ARCHIVO</td>" +
                 "<td style='width: 100px; height: 21px;'>TOTAL_REGISTROS</td>" +
                "<td style='width: 100px; height: 15px;'>REGISTROS_COPIADOS</td>" +
                "<td style='width: 100px; height: 15px;'>FECHA_ULTIMA_CARGA</td>" +
                "<td style='width: 100px; height: 15px;'>PERIODO</td>";
            str = str + "</tr>";
            string str2 = "";
            foreach (Archivos detalle in pListaArchivos)
            {
                object[] objArray1 = new object[] { str2,
                    "<tr><td style='width: 60px'>", detalle.pNombreArchivo,
                    "</td><td style='width: 240px'>", detalle.pTotalRegistro,
                    "</td><td style='width: 40px'>", detalle.pRegistrosCopiado,
                    "</td><td style='width: 60px'>", detalle.pFechaUtilmaCarga,
                    "</td><td style='width: 40px'>",  detalle.pPeriodo  };
                str2 = string.Concat(objArray1);
                str2 = str2 + "</tr>";
            }
            return (str + str2 + "</table>");
        }

    }
}

public class Archivos
{

    public string pNombreArchivo { get; set; }
    public string pTotalRegistro { get; set; }
    public string pRegistrosCopiado { get; set; }
    public string pFechaUtilmaCarga { get; set; }
    public string pPeriodo { get; set; }

}

public class LoginPost
{
    public string email { get; set; }
    public string password { get; set; }     
}

public class ResultadoPost
{
    public string token { get; set; }
    //public string password { get; set; }
}

public class EnvioPost
{
    public string canal { get; set; }
    public string codConvenio { get; set; }
    public string codOperacion { get; set; }
    public string empresaOrigen { get; set; }
    public string fechaVencimiento { get; set; }
    public string idCliente { get; set; }
    public string idOperacion { get; set; }
    public string importe { get; set; }
    public string moneda { get; set; }
    public string nombreCliente { get; set; }
 //   public string[] listadoCodigoCobranza { get; set; }

}


public class EnvioPostLote
{
    public string canal { get; set; }
    public string codConvenio { get; set; }
    public string codOperacion { get; set; }
    public string empresaOrigen { get; set; }
 
    public string idOperacion { get; set; }
    
    public string[] listadoCodigoCobranza { get; set; }

}


public class RespuestaPostEnvio
{
    public string message { get; set; }
    public string id { get; set; }
    public string fechaRegistro { get; set; }
    public string transaccionId { get; set; }
}

public class RespuestaPostEnvioMultipleFiltro
{

    public string message { get; set; }
    public string fechaConsulta { get; set; }
    public string transaccionId { get; set; }

    public List<data> data { get; set; }


}

public class echo
{
    

}

public class data
{
    public string moneda { get; set; }
    public string codOperacion { get; set; }
    public string empresaOrigen { get; set; }
    public string nombreProveedorPago { get; set; }    
    public string canal { get; set; }
    public string nombreCliente { get; set; }
    public string estado { get; set; }
    public string idCliente { get; set; }
    public string importeOriginal { get; set; }
    public string descripcionCobranza { get; set; }
    public string idOperacion { get; set; }
    public string fechaVencimiento { get; set; }
    public string fechaRegistro { get; set; }
    public string idTransaccionRegistro { get; set; }
    public string importe { get; set; }
    public string fechaActualizacionEstado { get; set; }
    public string codCliente { get; set; }
    public string codConvenio { get; set; }
    public string fechaVencimientoOriginal { get; set; }
    public string id { get; set; }
    public string fechaProcesoPago { get; set; }
    

}