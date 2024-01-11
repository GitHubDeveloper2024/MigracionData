using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

namespace MigracionData
{
    public class Correo
    {
        public bool fn_EnviarDemonio(string pCorreoCredencial, string pPasswordCredencial, string pSmtpCliente, int pPuerto,
        string pRemitente, string pTituloInterno, string pAsunto, string pCuerpo, List<CorreoDestinatario> pListaDestinatario,
        bool Adjuntar, List<ComprobanteAdjunto> pListaComprobanteAdjunto, List<CorreoConCopia> pListaCorreoCopiar, List<CorreoOculto> pListaCorreosOcultos, bool IsBodyHtml, bool Asincronico, bool SSL)
        {
            string RutaActual = "";
            try
            {
                //Configuración del Mensaje
                System.Net.Mail.MailMessage mail = new System.Net.Mail.MailMessage();
                //System.Net.Mail.SmtpClient SmtpServer = new SmtpClient(pSmtpCliente); // 


                //   MailAddress oMailAddress = null;

                //MailAddressCollection oListaMailAddressCollection = new MailAddressCollection();
                //Especificamos el correo desde el que se enviará el Email y el nombre de la persona que lo envía
                mail.From = new MailAddress(pRemitente, pTituloInterno, Encoding.UTF8);

                mail.IsBodyHtml = IsBodyHtml;
                //Aquí ponemos el asunto del correo
                mail.Subject = pAsunto; //"Se realizó la Aprobación de la Orden de Compra: 001-0009898";
                //Aquí ponemos el mensaje que incluirá el correo
                mail.Body = pCuerpo;
                //Especificamos a quien enviaremos el Email, no es necesario que sea Gmail, puede ser cualquier otro proveedor

                if (pListaDestinatario != null)
                {
                    foreach (CorreoDestinatario pDestinatario in pListaDestinatario)
                    {
                        try
                        {
                            pDestinatario.Descripcion = pDestinatario.Descripcion.Replace(";", ",");
                            mail.To.Add(pDestinatario.Descripcion);
                        }
                        catch (Exception)
                        {
                        }
                    }
                }


                if (pListaCorreoCopiar != null)
                {
                    foreach (CorreoConCopia pConCopia in pListaCorreoCopiar)
                        mail.CC.Add(pConCopia.Descripcion);
                }


                if (pListaCorreosOcultos != null)
                {
                    foreach (CorreoOculto pConCopiaOculta in pListaCorreosOcultos)
                        mail.Bcc.Add(pConCopiaOculta.Descripcion);
                }


                /*
                BOCGeneric oBoGeneric = new BOCGeneric();
                foreach(DataRow oRows in oBoGeneric.fn_ObtenerResultado("Pa_Compania_Buscar",).Rows)
                {


                }*/

                RutaActual = @"C:\inetpub\BACKUP\Web\Web\";

                if (pListaComprobanteAdjunto != null)
                {
                    foreach (ComprobanteAdjunto pDireccionAdjuntar in pListaComprobanteAdjunto)
                    {
                        /// throw new Exception(RutaActual + (pDireccionAdjuntar.Ruta));

                        mail.Attachments.Add(new Attachment(pDireccionAdjuntar.Ruta));
                    }
                }
                //dejame revisar primnero el codigo

                if (Asincronico)
                {
                    SmtpServer.Host = pSmtpCliente;

                    //Configuracion del SMTP
                    SmtpServer.Port = pPuerto; //Puerto que utiliza Gmail para sus servicios
                    //Especificamos las credenciales con las que enviaremos el mail

                    SmtpServer.Credentials = new System.Net.NetworkCredential(pCorreoCredencial, pPasswordCredencial);
                    SmtpServer.EnableSsl = true;
                    //SmtpServer.Send(mail);
                    //SmtpServer.SendCompleted += new SendCompletedEventHandler(SmtpServer_SendCompleted);
                    SmtpServer.SendAsync(mail, null); //has a qui ya es asincronico pero si quieres que te reporte de cosas si fallo o e progso del envio tiene 
                    //usar eventos // ese null puede ser un objeto x con datos adicionales para que lo use aqui

                }
                else
                {

                    SmtpServer.Host = pSmtpCliente;
                    //Configuracion del SMTP
                    SmtpServer.Port = pPuerto; //Puerto que utiliza Gmail para sus servicios
                    //Especificamos las credenciales con las que enviaremos el mail
                    //SmtpServer.Credentials = new System.Net.NetworkCredential("coxdeveloperc@gmail.com", "ereccion");
                    SmtpServer.Credentials = new System.Net.NetworkCredential(pCorreoCredencial, pPasswordCredencial);
                    SmtpServer.EnableSsl = SSL;
                    SmtpServer.Send(mail);

                }

                //ya mira todo los o bjetos de procesos la moyoria del .net framworf 2.0 tienen un metodo para hacer sus cosas
                //pero esa es la forma sincronna esat es la forma asincrona SendAsync como se usa mira 
                return true;
            }
            catch (Exception ex)
            {
                throw ex;

            }
        }

        private static System.Net.Mail.SmtpClient instance;

        public static System.Net.Mail.SmtpClient SmtpServer
        {
            get
            {
                if (instance == null)
                {
                    instance = new System.Net.Mail.SmtpClient();
                }
                return instance;
            }
        }
    }


}

public class ComprobanteAdjunto
{
    public string Ruta { get; set; }
}

public class CorreoDestinatario
{
    public string Descripcion { get; set; }
}

public class CorreoOculto
{
    public string Descripcion { get; set; }
}

public class CorreoConCopia
{
    public string Descripcion { get; set; }
}



