using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MigracionData.Modelo
{
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
    public class EnvioPost2
    {
        public string canal { get; set; }
        public string codConvenio { get; set; }
        public string codOperacion { get; set; }
        public string empresaOrigen { get; set; }
        public string idOperacion { get; set; }



    }

    public class EnvioPost4
    {
        public string canal { get; set; }
        public string codOperacion { get; set; }
        public string codConvenio { get; set; }
        public string idTrazabilidad { get; set; }
        public string codigoCobranzaAsociada { get; set; }
        public string fechaVencimiento { get; set; }
    }
    public class EnvioPost3
    {
        public string canal { get; set; }
        public string codOperacion { get; set; }
        public string codConvenio { get; set; }
        public string idTrazabilidad { get; set; }
        public string codigoCobranzaAsociada { get; set; }
        public string monto { get; set; }
        public string moneda { get; set; }
        public string referencia { get; set; }


    }
}
