using hbsis_avro;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;

namespace Tests
{
    [TestClass]
    public class UnitTest1
    {
        private string json_schema;
        private string json_data;
        private dynamic obj_data;

        public UnitTest1()
        {
            this.json_schema = File.ReadAllText("json_schema.json");
            this.json_data = File.ReadAllText("json_data.json");
            this.obj_data = new TestClass
            {
                category = "Test",
                severity = "WARNING",
                timestamp = 4000
            };
        }
        [TestMethod]
        public void TestMethod1()
        {
            var ao = new AvroObject(this.json_data);

           ao = new AvroObject(this.obj_data, this.json_schema);

            ao = new AvroObject(this.json_data, this.json_schema);
        }
    }

    public class TestClass
    {
        public string category { get; set; }
        public string severity { get; set; }
        public long timestamp { get; set; }
    }
}
