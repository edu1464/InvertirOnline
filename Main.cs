using System;
using System.IO;
using WinSCP;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Configuration;
using System.Net.Http;
using System.Net.Http.Headers;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            // Cargar la lista de IDs
            List<string> documentsToDownload = GetDocumentsFromSql();

            // Crear una cola para almacenar los IDs
            var idQueue = new ConcurrentQueue<string>(documentsToDownload);

            // Crear 5 tareas para procesar los IDs
            var tasks = new Task[5];
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(() => Work(idQueue));
            }

            // Esperar a que todas las tareas terminen
            Task.WaitAll(tasks);

            Console.WriteLine("Procesamiento completo.");
        }

        static List<string> GetDocumentsFromSql()
        {
            // Obtener documentos que no han sido descargados
            string connectionString = ConfigurationManager.AppSettings["connectionString"];

            List<string> documentsToDownload = new List<string>();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                using (SqlCommand command = new SqlCommand("SELECT [Internal ID] FROM ExportedDocuments WHERE ISNULL(IsDownloaded, 0) = 0", connection))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string documentId = reader.GetString(0);

                            documentsToDownload.Add(documentId);

                            // Agregar archivo a lista de archivos a cargar
                            //documentsToUpload.Add(Path.Combine(localDirectory, documentId));
                        }
                    }
                }
            }

            return documentsToDownload;
        }

        static void Work(ConcurrentQueue<string> idQueue)
        {
            //Obtener valores globales
            string connectionString = ConfigurationManager.AppSettings["connectionString"];
            string localDirectory = ConfigurationManager.AppSettings["localDirectory"];

            // Configurar sesión de WinSCP
            SessionOptions sessionOptions = new SessionOptions
            {
                Protocol = Protocol.Sftp,
                HostName = ConfigurationManager.AppSettings["hostName"],
                UserName = ConfigurationManager.AppSettings["userName"],
                Password = ConfigurationManager.AppSettings["password"],
                SshHostKeyFingerprint = ConfigurationManager.AppSettings["sshHostKeyFingerprint"],

            };



            string documentId = string.Empty;
            while (idQueue.TryDequeue(out documentId))
            {
                // Cargar archivo
                bool success = false;

                // Descargar documento
                string GetDocumentSuccess = GetDocument(documentId, localDirectory);

                // Actualizar campo IsDownloaded a 1
                if (!string.IsNullOrEmpty(GetDocumentSuccess))
                {
                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        connection.Open();
                        using (SqlCommand updateCommand = new SqlCommand("UPDATE ExportedDocuments SET IsDownloaded = 1 WHERE [Internal ID] = @ID", connection))
                        {
                            updateCommand.Parameters.AddWithValue("@ID", documentId);

                            int rowsAffected = updateCommand.ExecuteNonQuery();

                            if (rowsAffected > 0)
                            {
                                Console.WriteLine("Documento descargado y campo IsDownloaded actualizado");
                            }
                            else
                            {
                                Console.WriteLine("No se pudo actualizar el campo IsDownloaded en la tabla ExportedDocuments");
                            }
                        }
                    }

                    string filePath = Path.Combine(localDirectory, GetDocumentSuccess);
                    success = LoadFile(sessionOptions, filePath);

                    if (success)
                    {
                        File.Delete(filePath);
                    }
                    else
                    {
                        Console.WriteLine("Error al cargar archivo");
                    }
                }                                
            }
        }

        static bool LoadFile(SessionOptions sessionOptions, string localFilePath)
        {
            using (Session session = new Session())
            {
                try
                {
                    // Conectar a servidor FTP
                    session.Open(sessionOptions);

                    // Consultar si el archivo ha sido subido previamente
                    bool isUploaded = GetIsUploaded(localFilePath);

                    // Si el archivo no ha sido subido previamente, cargarlo
                    if (!isUploaded)
                    {
                        // Crear carpetas de destino si no existen
                        string remoteFilePath = GetRemoteFileName(localFilePath);
                        string directoryName = RemotePath.GetDirectoryName(remoteFilePath);
                        if (!session.FileExists(directoryName))
                        {
                            session.CreateDirectory(directoryName);
                        }

                        // Cargar archivo
                        TransferOptions transferOptions = new TransferOptions();
                        transferOptions.PreserveTimestamp = false;
                        transferOptions.TransferMode = TransferMode.Binary; // Opcional, dependiendo del tipo de archivo
                        TransferOperationResult transferResult;
                        transferResult = session.PutFiles(localFilePath, remoteFilePath, false, transferOptions);

                        if (transferResult.IsSuccess)
                        {
                            // Actualizar campo IsUploaded en la base de datos
                            SetIsUploaded(localFilePath, 1);
                            return true;
                        }
                        else
                        {
                            Console.WriteLine("Error al cargar archivo");
                            return false;
                        }
                    }
                    else
                    {
                        Console.WriteLine("El archivo ya ha sido cargado previamente");
                        return false;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error al cargar archivo: " + e.Message);
                    return false;
                }
                finally
                {
                    session.Close();
                }
            }
        }

        static bool GetIsUploaded(string localFilePath)
        {
            string connectionString = ConfigurationManager.AppSettings["connectionString"];
            bool isUploaded = false;

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                using (SqlCommand command = new SqlCommand("SELECT IsUploaded FROM ExportedDocuments WHERE [Internal ID] = @ID", connection))
                {
                    command.Parameters.AddWithValue("@ID", Path.GetFileNameWithoutExtension(localFilePath));

                    SqlDataReader reader = command.ExecuteReader();

                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            isUploaded = (bool)reader["IsUploaded"];
                        }
                    }
                    reader.Close();
                }
            }

            return isUploaded;
        }

        static void SetIsUploaded(string localFilePath, int isUploaded)
        {
            string connectionString = ConfigurationManager.AppSettings["connectionString"];

            // Actualizar el campo IsUploaded a true en la tabla ExportedDocuments            
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                using (SqlCommand command = new SqlCommand("UPDATE ExportedDocuments SET IsUploaded = @IsUploaded WHERE [Internal ID] = @ID", connection))
                {
                    command.Parameters.AddWithValue("@IsUploaded", true);
                    command.Parameters.AddWithValue("@ID", Path.GetFileNameWithoutExtension(localFilePath));

                    int rowsAffected = command.ExecuteNonQuery();

                    if (rowsAffected > 0)
                    {
                        Console.WriteLine("Archivo cargado exitosamente");
                    }
                    else
                    {
                        Console.WriteLine("No se pudo actualizar el campo IsUploaded en la tabla ExportedDocuments");
                    }
                }
            }
        }


        static string GetRemoteFileName(string localFilePath)
        {
            string connectionString = ConfigurationManager.AppSettings["connectionString"];
            string remoteDirectory = ConfigurationManager.AppSettings["remoteDirectory"];
            string fileName = Path.GetFileName(localFilePath);
            string year = string.Empty;
            string portal = string.Empty;
            string docStatus = string.Empty;

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                using (SqlCommand command = new SqlCommand("SELECT YEAR([Date Created]) AS [DateCreated], [Show on Portal] AS [ShowOnPortal], [Document Status] AS [DocumentStatus] FROM ExportedDocuments WHERE [Internal ID] = @ID", connection))
                {
                    command.Parameters.AddWithValue("@ID", Path.GetFileNameWithoutExtension(localFilePath));

                    SqlDataReader reader = command.ExecuteReader();

                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            year = reader["DateCreated"].ToString();
                            portal = reader["ShowOnPortal"].ToString();
                            docStatus = reader["DocumentStatus"].ToString();
                        }
                    }
                    reader.Close();
                }
            }

            string dirPortal = string.Empty;

            if (portal == "True" && docStatus == "Approved for Portal")
            {
                dirPortal = "Portal Documents";
            }
            else
            {
                dirPortal = "Non Portal Documents";
            }         

            return string.Format("{0}{1}/{2}/{3}", remoteDirectory, dirPortal, year, fileName);
        }

        static string GetDocument(string documentId, string path)
        {
            string accessToken = ConfigurationManager.AppSettings["accessToken"];

            string apiUrl = "https://api.test.com/api/v2.1/Entity/Document/" + documentId + "?mobile=false";

            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            client.DefaultRequestHeaders.Add("x-columns", "Extension");

            HttpResponseMessage response = client.GetAsync(apiUrl).Result;

            if (response.IsSuccessStatusCode)
            {
                string result = response.Content.ReadAsStringAsync().Result;
                JObject resultJson = JObject.Parse(result);
                string contentBase64 = resultJson["data"]["_content"].ToString();
                string extension = resultJson["data"]["Extension"].ToString();
                byte[] contentBytes = Convert.FromBase64String(contentBase64);
                string filePath = Path.Combine(path, documentId + extension);
                File.WriteAllBytes(filePath, contentBytes);
                return documentId + extension;
            }
            else
            {
                Console.WriteLine("Error al obtener documento");
                return null;
            }
        }

    }
}
