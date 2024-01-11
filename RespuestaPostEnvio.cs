using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MigracionData
{
    public class RespuestaPostEnvio
    {
        public string JSON { get; set; }
        public string message { get; set; }
        public string id { get; set; }
        public string fechaRegistro { get; set; }
        public string transaccionId { get; set; }
        public string numeroOperacion { get; set; }

    }
}
