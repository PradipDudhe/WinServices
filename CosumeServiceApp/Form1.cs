using System.Windows.Forms;
using WSDLDemoService;

namespace CosumeServiceApp
{
    public partial class Form1 : Form
    {
        ClientServiceProducer serviceProducer = new ClientServiceProducer();
        public Form1()
        {

           
            InitializeComponent();
        }
       public void GetData()
        {
          int test=  serviceProducer.Add(10, 10);
        }
    }
}
