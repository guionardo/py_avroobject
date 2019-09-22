using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using Avro;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;
using Microsoft.Hadoop.Avro;
using Microsoft.Hadoop.Avro.Container;
using Microsoft.Hadoop.Avro.Schema;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace hbsis_avro
{
    public class AvroObject
    {
        private byte[] _avro_data;
        private string _last_error = null;

        private object _object_data = null;

        private TypeSchema _schema = null;
        public AvroObject(byte[] avroBinary)
        {
            //TODO: Construtor using avroBinary   
            MemoryStream ms = new MemoryStream(avroBinary);
            using (var reader = AvroContainer.CreateGenericReader(ms))
            {
                while (reader.MoveNext())
                {
                    foreach (dynamic record in reader.Current.Objects)
                    {

                    }
                }
                _schema = reader.Schema;
            }
        }

        public AvroObject(string json)
        {
            //TODO: Constructor using JSON string (no schema)
            if (!CreateObject(json, out Exception exception))
                throw exception;
        }

        public AvroObject(string json_object, string json_schema)
        {
            //TODO: Constructor using JSON string with JSON schema
            if (!CreateSchema(json_schema, out Exception exception))
                throw exception;
            if (!CreateObject(json_object, out exception))
                throw exception;
            if (!Serialize(this._object_data, this._schema, out exception))
                throw exception;
        }

        public AvroObject(string json_object, TypeSchema schema)
        {
            if (!CreateObject(json_object, out Exception exception))
                throw exception;
            this._schema = schema;
            if (!Serialize(this._object_data, this._schema, out exception))
                throw exception;
        }

        public AvroObject(object structure)
        {
            //TODO: Constructor using dynamic structure
        }

        public AvroObject(object structure, string json_schema)
        {
            if (!CreateSchema(json_schema, out Exception exception))
                throw exception;
            if (!Serialize(structure, this._schema, out exception))
                throw exception;
            //
        }

        private bool CreateObject(string json_object, out Exception exception)
        {
            exception = null;
            try
            {
                var obj = JsonConvert.DeserializeObject<dynamic>(json_object);

                _object_data = obj;
            }
            catch (Exception e)
            {
                exception = e;
            }
            return exception == null;
        }

        private bool CreateSchema(string json_schema, out Exception exception)
        {
            exception = null;
            try
            {
                var schema = TypeSchema.Create(json_schema);
                _schema = schema;
            }
            catch (Exception e)
            {
                exception = e;
            }
            return exception == null;
        }

        private bool Serialize(object object_data, TypeSchema schema, out Exception exception)
        {
            exception = null;
            try
            {
                var serializer = AvroSerializer.CreateGeneric(schema.ToString());
                var stream = new MemoryStream();
                serializer.Serialize(stream, _object_data);
                _avro_data = new byte[stream.Length];
                stream.Seek(0, SeekOrigin.Begin);
                if (stream.Read(_avro_data, 0, (int)stream.Length) != stream.Length)
                    exception = new Exception("Serializing error!");
            }
            catch (Exception e)
            {
                exception = e;
            }
            return exception == null;
        }
    }
}
