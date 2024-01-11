using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MigracionData.Modelo
{
    public class NotaCreditoOutput
    {
        public string message { get; set; }
        public string id { get; set; }
        public string fechaRegistro { get; set; }
        public string transaccionId { get; set; }

        public echo echo { get; set; }


    }

    public class echo
    {
        public headers headers { get; set; }
    }

    public class headers
    {
        public string authorization { get; set; }
    }
}
