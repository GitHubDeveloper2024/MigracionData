using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MigracionData
{
    public class CuponInput
    {
        public string email_token { get; set; }
        public string clave_token { get; set; }
        public string Api_token { get; set; }
        public string Api_Cupon { get; set; }
        public string canal { get; set; }
        public string fechaVencimiento { get; set; }
        public string ImportePendiente { get; set; }
        public string idCliente { get; set; }
        public string Socio { get; set; }
        public string idOperacion { get; set; }
        public string codConvenio { get; set; }
        public string codOperacion { get; set; }
        public string empresaOrigen { get; set; }

        public string Api_Patch { get; set; }


    }
}
