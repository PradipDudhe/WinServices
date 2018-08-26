using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Data.OleDb;
using WinServices.ServiceReference1;

namespace EMEEClientWinService
{
    partial class EMEEClientAttendanceService : ServiceBase
    {
        IService1 service1;
        #region DECLARATION
       
        private System.Timers.Timer EMEETimer;

        ExternalServiceBL ObjExternalServiceBLExternalServiceBL
        ARMachineDetails[] ARMAchineList;
        ARMachineDetails3[] ARMAchineList3;
        SchedulerFileDetails[] SchedulerFileList;
        private List<ARMachineFile> ARMachineFileList;
        private List<IntegratedARMFile> IntegratedARMFileList;
        private List<ARMachineFileData> ARMachineFileDataBatchList;
        private Int64 SUHId;
        private Int64 SFUId;
        private DataTable DT_AttendanceExcelData;
        private DataTable DT_IntegratedAttendanceData;
        ARMachineFile objARMachineFileGbl;
        IntegratedARMFile objIntegratedARMFileGbl;

        private bool IS_SETTING = false;

        private int TIMEOUT_MINUTES = 2;
        private int SETTING_READ_INTERVAL_MINUTES;
        private int? SWIPE_UPLOAD_INTERVAL_MINUTES;
        private int UPLOADBATCHSIZE;

        private DateTime? SWIPE_DATA_UPLOAD_TIME;
        private DateTime? SETTINGLASTUPDATEDTIME = null;
        private DateTime? SWIPELASTUPLOADEDTIME = null;

        private int UPLOADING_TYPE = 0;
        private string SCHEDULER_DATASOURCE_PATH = "";

        private EMEEServiceExtBL.AttendanceRead.IAttendanceUpload _GblobjAttendnaceUpload;

        private int TEMP_ROW_START_POS_3 = 999;

        #endregion

        #region CCONSTRUCTOR

        public EMEEClientAttendanceService()
        {
            service1 = new service1();
            InitializeComponent();
            ObjExternalServiceBL = new ExternalServiceBL();
            //objARMachineFileGbl = new ARMachineFile();
        }

        #endregion

        #region EVENTS

        protected override void OnStart(string[] args)
        {
            IService1 service1 = new Service1();
            service1.
            service1.
            bool isTimerEnabed = false;
            try
            {
                JobLogging("EMEE Attendance Service successfully STARTED.");
                InsertOperationLog(false, "EMEE Attendance Service successfully STARTED.", "SA", "OnStart", "");



                if (ReadSchedulerSetting())
                {
                    JobLogging("ServiceExt: Scheduler details read successfully.");
                    //int timeIntervalMin = Convert.ToInt16(ConfigurationManager.AppSettings["ServiceTimeIntervalMins"].ToString());
                    EMEETimer = new System.Timers.Timer(TIMEOUT_MINUTES * 60 * 1000);
                    EMEETimer.Elapsed += new System.Timers.ElapsedEventHandler(Timer_Elapsed);
                    EMEETimer.Enabled = true;
                    isTimerEnabed = true;
                }
                else
                {
                    JobLogging("ServiceExt: Scheduler details reading failed.");
                    InsertOperationLog(true, "ServiceExt: Scheduler details reading failed.", "SA", "OnStart", "");
                }

            }
            catch (Exception ex)
            {
                string message = "Error:OnStart()-" + ex.Message;
                JobLogging(message);
                InsertOperationLog(true, message, "SA", "OnStart", "");
            }

        }

        private void Timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            EMEETimer.Enabled = false;

            try
            {
                // objARMachineFileGbl = new ARMachineFile();

                if (ConfigurationManager.AppSettings["IsLoggingEnabled"] != null)
                {
                    if (ConfigurationManager.AppSettings["IsLoggingEnabled"].ToString() == "Y")
                    {
                        JobLogging("EMEE Attendance Service timeout event fired successfully.");
                    }
                }


                ReadSchedulerSettingsJob();


                UploadExcelDataJob();

                //objARMachineFileGbl = new ARMachineFile();
            }
            catch (Exception ex)
            {
                string message = "Error:Timer_Elapsed()-" + ex.Message;
                JobLogging(message);
                InsertOperationLog(true, message, "TO", "Timer_Elapsed", "");


            }
            //finally
            //{
            //    EMEETimer.Enabled = true;
            //}


            EMEETimer.Enabled = true;
        }

        protected override void OnStop()
        {
            try
            {
                //JobLogging("EMEE Attendance Service successfully STOPPED.");
                InsertOperationLog(false, "EMEE Attendance Service successfully STOPPED..", "SP", "OnStop", "");
            }
            catch (Exception ex)
            {
                string message = "Error:OnStop()-" + ex.Message;
                //JobLogging(message);
                //InsertOperationLog(true, message, "SP", "OnStop", "");
            }
        }

        #endregion


        #region JOBS 

        private void ReadSchedulerSettingsJob()
        {
            try
            {
                bool isSettingReadIntervalElapsed = false;
                if (SETTINGLASTUPDATEDTIME.HasValue == false)
                {
                    isSettingReadIntervalElapsed = true;
                }
                else
                {
                    if (SETTINGLASTUPDATEDTIME.Value.AddMinutes(SETTING_READ_INTERVAL_MINUTES) <= DateTime.Now)
                    {
                        isSettingReadIntervalElapsed = true;
                    }
                }
                if (isSettingReadIntervalElapsed)
                {
                    //SETTINGLASTUPDATEDTIME = DateTime.Now;
                    if (ReadSchedulerSetting())
                    {
                        JobLogging("ServiceExt: Scheduler details was read successfully in timeout event.");
                    }
                    else
                    {
                        JobLogging("ServiceExt: Scheduler details reading failed in timeout event.");
                    }
                    SETTINGLASTUPDATEDTIME = DateTime.Now;
                }
            }
            catch (Exception ex)
            {
                throw new Exception("ReadSchedulerSettingsJob()-" + ex.Message, ex);
            }
        }

        private void UploadExcelDataJob()
        {
            try
            {
                bool isSwipeUploadIntervalElapsed = false;
                if (SWIPELASTUPLOADEDTIME.HasValue == false)
                {
                    isSwipeUploadIntervalElapsed = true;
                }
                else
                {
                    if (SWIPELASTUPLOADEDTIME.Value.AddMinutes(SWIPE_UPLOAD_INTERVAL_MINUTES.Value) <= DateTime.Now)
                    {
                        isSwipeUploadIntervalElapsed = true;
                    }
                }
                if (isSwipeUploadIntervalElapsed)
                {
                    SWIPELASTUPLOADEDTIME = DateTime.Now;
                    if (UploadSwipeDataTypes())
                    {
                        JobLogging("ServiceExt: Swipe data was uploaded successfully in timeout event.");
                    }
                    else
                    {
                        JobLogging("ServiceExt: Swipe data uploading failed in timeout event.");
                    }
                    SWIPELASTUPLOADEDTIME = DateTime.Now;
                }
            }
            catch (Exception ex)
            {
                throw new Exception("UploadExcelDataJob()-" + ex.Message, ex);
            }
        }

        private bool UploadSwipeDataTypes()
        {
            bool status = false;
            try
            {
                switch (UPLOADING_TYPE)
                {
                    case 1:
                        ReadSchedulerSettingsJob();
                        status = UploadSwipeData1();
                        break;
                    case 2:
                        ReadSchedulerSettingsJob();
                        status = UploadSwipeData2();
                        break;
                    case 3:
                        if (CheckFileExists3())
                        {
                            ReadSchedulerSettingsJob();
                            UploadSwipeData3();
                        }
                        break;
                    default:
                        if (_GblobjAttendnaceUpload == null)
                        {
                            _GblobjAttendnaceUpload = EMEEServiceExtBL.AttendanceRead.AttendanceUploadFactory.GetAttendanceUploadObj(UPLOADING_TYPE, UPLOADBATCHSIZE);
                        }
                        _GblobjAttendnaceUpload.UploadSwipeData();
                        break;

                }
            }
            catch (Exception ex)
            {
                throw new Exception("UploadSwipeDataTypes()-" + ex.Message, ex);
            }
            return status;
        }


        #endregion

        #region METHODS

        #region ReadSchedulerSetting

        private bool ReadSchedulerSetting()
        {
            bool status = false;
            try
            {
                SchedulerSettings objSchedulerSertting = ObjExternalServiceBL.GetSchedulerSettings();
                if (objSchedulerSertting.SchedulerId > 0)
                {
                    IS_SETTING = true;
                    status = true;
                    TIMEOUT_MINUTES = objSchedulerSertting.TimeoutMinutes;
                    SETTING_READ_INTERVAL_MINUTES = objSchedulerSertting.SettingReadIntevalMinutes;
                    SWIPE_UPLOAD_INTERVAL_MINUTES = objSchedulerSertting.UploadingIntervalMinutes;
                    UPLOADBATCHSIZE = objSchedulerSertting.UploadBatchSize;
                    UPLOADING_TYPE = objSchedulerSertting.UploadingType;
                    SCHEDULER_DATASOURCE_PATH = objSchedulerSertting.DatasourcePath;
                    if (objSchedulerSertting.UploadingTime.Trim().Length > 0)
                    {
                        SWIPE_DATA_UPLOAD_TIME = Convert.ToDateTime(DateTime.Today.ToString("dd-MM-yyyy") + " " + objSchedulerSertting.UploadingTime);
                    }
                    else
                    {
                        SWIPE_DATA_UPLOAD_TIME = null;
                    }

                    SETTINGLASTUPDATEDTIME = DateTime.Now;
                }
            }
            catch (Exception ex)
            {
                throw new Exception("ReadSchedulerSetting()-" + ex.Message, ex);
            }
            return status;
        }

        #endregion

        #region UploadSwipeData1


        private bool UploadSwipeData1()
        {
            bool status = false;
            try
            {
                ARMAchineList = null;
                SUHId = 0;
                ARMachineFileList = new List<ARMachineFile>();
                DT_AttendanceExcelData = new DataTable();
                ARMachineFileDataBatchList = new List<ARMachineFileData>();

                //if (!ObjExternalServiceBL.GetSchedulerErrorPending())
                if (true)
                {
                    if (ReadARMachineList())
                    {
                        if (ReadFileListFromDisk())
                        {
                            int countARMListi = 0;
                            if (ARMAchineList != null) countARMListi = ARMAchineList.Length;

                            int countReadFileListi = 0;
                            if (ARMachineFileList != null) countReadFileListi = ARMachineFileList.Count;

                            InsertOperationLog(false, "ARMachine Count:" + countARMListi.ToString() + " File count:" + countReadFileListi, "RD", "ReadFileListFromDisk()", "");
                            if (InsertSchedulerOperationHeader())
                            {

                                for (int armFileCount = 0; armFileCount < ARMachineFileList.Count; armFileCount++)
                                {
                                    DT_AttendanceExcelData = null;
                                    objARMachineFileGbl = ARMachineFileList[armFileCount];
                                    if (ReadSwipeDataFromExcel(objARMachineFileGbl))
                                    {
                                        if (objARMachineFileGbl.IsReadSuccessful)
                                        {
                                            ARMachineFileDataBatchList = new List<ARMachineFileData>();
                                            if (ValidateExcelSwipeData(objARMachineFileGbl))
                                            {
                                                bool isLastFile = false;
                                                if (armFileCount == ARMachineFileList.Count - 1)
                                                {
                                                    isLastFile = true;
                                                }
                                                InsertAttendanceSwipeData(objARMachineFileGbl, isLastFile);
                                            }
                                            else
                                            {
                                                InsertOperationLog(true, objARMachineFileGbl.Remark, "VS", "ValidateExcelSwipeData()", objARMachineFileGbl.ExcelFilePath);
                                            }

                                        }
                                    }
                                    else
                                    {
                                        InsertOperationLog(true, objARMachineFileGbl.Remark, "RS", "ReadSwipeDataFromExcel()", objARMachineFileGbl.ExcelFilePath);
                                    }

                                    objARMachineFileGbl = new ARMachineFile();
                                }

                            }
                            else
                            {
                                int countARMList = 0;
                                if (ARMAchineList != null) countARMList = ARMAchineList.Length;

                                int countReadFileList = 0;
                                if (ARMachineFileList != null) countReadFileList = ARMachineFileList.Count;

                                InsertOperationLog(true, "Header insertion failed." + countARMList.ToString() + "," + countReadFileList.ToString(), "UH", "InsertSchedulerOperationHeader()", "");
                            }

                        }
                        else
                        {
                            int countARMList = 0;
                            if (ARMAchineList != null) countARMList = ARMAchineList.Length;

                            int countReadFileList = 0;
                            if (ARMachineFileList != null) countReadFileList = ARMachineFileList.Count;

                            InsertOperationLog(false, "No files found." + countARMList.ToString() + "," + countReadFileList.ToString(), "RD", "ReadFileListFromDisk()", "");

                        }
                    }
                }
                else
                {
                    InsertOperationLog(false, "Error is pending. So cannot proceed.", "TO", "GetSchedulerErrorPending()", "");
                }
            }
            catch (Exception ex)
            {
                throw new Exception("UploadSwipeData1()-" + ex.Message, ex);
            }
            return status;
        }

        private bool ReadFileListFromDisk()
        {
            bool status = false;
            DateTime checkFileDate = DateTime.Today;
            try
            {
                ARMachineFileList = new List<ARMachineFile>();
                if (ARMAchineList != null)
                {
                    ARMachineFile objARMAchineFile;
                    for (int armCount = 0; armCount < ARMAchineList.Length; armCount++)
                    {
                        string _DatasourcePath = ARMAchineList[armCount].DatasourcePath;
                        if (Directory.Exists(_DatasourcePath))
                        {
                            IEnumerable<string> excelFileList = Directory.EnumerateFiles(_DatasourcePath);
                            foreach (string excelFilePath in excelFileList)
                            {
                                if (CheckDate(Path.GetFileNameWithoutExtension(excelFilePath), ref checkFileDate))
                                {
                                    status = true;
                                    objARMAchineFile = new ARMachineFile();
                                    objARMAchineFile.TenantId = ARMAchineList[armCount].TenantId;
                                    objARMAchineFile.LocationId = ARMAchineList[armCount].LocationId;
                                    objARMAchineFile.ARMachineId = ARMAchineList[armCount].ARMachineId;
                                    objARMAchineFile.SuccessFolderPath = ARMAchineList[armCount].SuccessFolderPath;
                                    objARMAchineFile.ErrorFolderPath = ARMAchineList[armCount].ErrorFolderPath;
                                    objARMAchineFile.TableName = ARMAchineList[armCount].TableName;
                                    objARMAchineFile.EmpIdCol = ARMAchineList[armCount].EmpIdColPos;
                                    objARMAchineFile.SwipeTimeColumn = ARMAchineList[armCount].SwipeTimeColPos;
                                    objARMAchineFile.ExcelFilePath = excelFilePath;
                                    objARMAchineFile.LastAttendanceDate = null;
                                    objARMAchineFile.BatchSize = UPLOADBATCHSIZE;
                                    if (ARMAchineList[armCount].LastAttendanceDate.Trim().Length > 0)
                                    {
                                        objARMAchineFile.LastAttendanceDate = Convert.ToDateTime(ARMAchineList[armCount].LastAttendanceDate);
                                    }
                                    ARMachineFileList.Add(objARMAchineFile);
                                }

                            }

                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("ReadFileListFromDisk()-" + ex.Message, ex);
            }
            return status;
        }

        private bool InsertSchedulerOperationHeader()
        {
            bool status = false;
            try
            {
                OperationHeader objOperationHeader = new OperationHeader();
                objOperationHeader.FileCount = ARMachineFileList.Count;
                objOperationHeader.ARMachineCount = ARMAchineList.Length;
                OperationStatus objOperationStatus = ObjExternalServiceBL.InsertSchedulerUploadingHeader(objOperationHeader);
                if (objOperationStatus.Status)
                {
                    SUHId = objOperationStatus.OperationId;
                    status = true;
                }
            }
            catch (Exception ex)
            {
                throw new Exception("InsertSchedulerOperationHeader()-" + ex.Message, ex);
            }
            return status;
        }

        private bool ReadSwipeDataFromExcel(ARMachineFile objARMachineFile)
        {

            DataSet ds = null;
            OleDbConnection oledbConn = new OleDbConnection();
            bool status = false;
            try
            {

                string GenAttendanceExcelConStr = ConfigurationManager.AppSettings["AttendanceExcelConStr"].ToString();
                string excelConStr = GenAttendanceExcelConStr.Replace("EMATTENDANCEFILEPATH", objARMachineFile.ExcelFilePath);
                oledbConn.ConnectionString = excelConStr;
                oledbConn.Open();
                OleDbCommand cmd = new OleDbCommand("SELECT * FROM [" + objARMachineFile.TableName + "$]", oledbConn);
                OleDbDataAdapter oleda = new OleDbDataAdapter();
                oleda.SelectCommand = cmd;
                ds = new DataSet("EMROOT");
                oleda.Fill(ds, "EMATTENDANCE");
                oledbConn.Close();

                if (ds.Tables.Count > 0)
                {
                    DT_AttendanceExcelData = ds.Tables[0];
                    objARMachineFile.IsReadSuccessful = true;
                    status = true;
                    objARMachineFile.ExcelFileName = Path.GetFileName(objARMachineFile.ExcelFilePath);
                    objARMachineFile.AttendanceDate = Convert.ToDateTime(Path.GetFileNameWithoutExtension(objARMachineFile.ExcelFilePath));
                }
            }
            catch (Exception ex)
            {
                oledbConn.Close();
                oledbConn.Dispose();

                string message = "Error:ReadSwipeDataFromExcel()-" + ex.Message;
                JobLogging(message);
                InsertOperationLog(true, message, "TO", "ReadSwipeDataFromExcel()", "");
            }
            finally
            {
                oledbConn.Close();
                oledbConn.Dispose();
            }
            return status;
        }

        private bool ValidateExcelSwipeData(ARMachineFile objARMachineFile)
        {
            bool status = true;
            try
            {
                bool isErrorFound = false;
                string arEmpId = "";
                string swipeDateTime = "";
                DateTime swipeDateTimeDate = DateTime.Now;
                DataRow dr;
                DataTable dtAttendance = GenerateAttendanceTable();
                ARMachineFileDataBatchList = new List<ARMachineFileData>();

                if (IsSwipeDataPresent(objARMachineFile))
                {

                    decimal dtRowCount = DT_AttendanceExcelData.Rows.Count;
                    decimal decBatchCount = dtRowCount / (decimal)objARMachineFile.BatchSize;
                    int maxBatchCount = (int)decBatchCount;
                    if (maxBatchCount < decBatchCount)
                    {
                        maxBatchCount++;
                    }

                    List<int> batchList = new List<int>();
                    for (int batchCount = 0; batchCount < maxBatchCount; batchCount++)
                    {
                        batchList.Add(objARMachineFile.BatchSize * batchCount);
                    }

                    int batchIndex = 0;

                    for (int rowCount = 0; rowCount < DT_AttendanceExcelData.Rows.Count; rowCount++)
                    {
                        if (rowCount == batchList[batchIndex])
                        {
                            dtAttendance = GenerateAttendanceTable();
                        }
                        arEmpId = DT_AttendanceExcelData.Rows[rowCount][objARMachineFile.EmpIdCol].ToString();
                        swipeDateTime = DT_AttendanceExcelData.Rows[rowCount][objARMachineFile.SwipeTimeColumn].ToString();
                        if (arEmpId.Trim().Length > 0)
                        {
                            if (CheckDate(swipeDateTime, ref swipeDateTimeDate))
                            {
                                if (isErrorFound == false)
                                {

                                    dr = dtAttendance.NewRow();
                                    dr["AR_Emp_Id"] = arEmpId;
                                    dr["Swipe_DateTime"] = swipeDateTimeDate.ToString("dd-MMM-yyyy HH:mm:ss");
                                    dr["Swipe_DateTime_Date"] = swipeDateTimeDate;
                                    dtAttendance.Rows.Add(dr);

                                }
                            }
                            else
                            {
                                isErrorFound = true;
                                status = false;
                                objARMachineFile.IsReadSuccessful = false;
                                objARMachineFile.Remark = "Invalid datetime.";
                                break;

                            }
                        }
                        if (batchIndex < maxBatchCount - 1)
                        {
                            if (rowCount == batchList[batchIndex + 1] - 1)
                            {
                                if (dtAttendance.Rows.Count > 0)
                                {
                                    ARMachineFileDataBatchList.Add(ConvertSwipeDataTableToXML(dtAttendance, batchIndex + 1));
                                }
                                batchIndex++;
                            }
                        }
                        else
                        {
                            if (rowCount == DT_AttendanceExcelData.Rows.Count - 1 && dtAttendance.Rows.Count > 0)
                            {

                                ARMachineFileDataBatchList.Add(ConvertSwipeDataTableToXML(dtAttendance, batchIndex + 1));
                                batchIndex++;
                            }
                        }


                    }
                }
                else
                {
                    dtAttendance = GenerateAttendanceTable();
                    ARMachineFileDataBatchList.Add(ConvertSwipeDataTableToXML(dtAttendance, 1));
                }



            }
            catch (Exception ex)
            {
                string message = "Error:ValidateExcelSwipeData()-" + ex.Message;
                JobLogging(message);
                InsertOperationLog(true, message, "TO", "ValidateExcelSwipeData()", "");
            }
            return status;
        }

        private bool IsSwipeDataPresent(ARMachineFile objARMachineFile)
        {
            bool status = false;
            try
            {
                if (DT_AttendanceExcelData.Columns.Count >= 2)
                {
                    if (DT_AttendanceExcelData.Rows.Count > 0)
                    {
                        string colEmp = DT_AttendanceExcelData.Columns[objARMachineFile.EmpIdCol].ColumnName;
                        DT_AttendanceExcelData.DefaultView.RowFilter = colEmp + "<>''";
                        if (DT_AttendanceExcelData.DefaultView.Count > 0)
                        {
                            status = true;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("IsSwipeDataPresent()-" + ex.Message, ex);
            }
            return status;
        }

        private bool InsertAttendanceSwipeData(ARMachineFile objARMachineFile, bool isLastFile)
        {
            bool status = false;
            try
            {
                ARMachineSwipeData objARMachineSwipeData = new ARMachineSwipeData();
                objARMachineSwipeData.TenantId = objARMachineFile.TenantId;
                objARMachineSwipeData.LocationId = objARMachineFile.LocationId;
                objARMachineSwipeData.ARMachineId = objARMachineFile.ARMachineId;
                objARMachineSwipeData.AttendanceDate = "";
                if (objARMachineFile.AttendanceDate.HasValue)
                {
                    objARMachineSwipeData.AttendanceDate = objARMachineFile.AttendanceDate.Value.ToString("dd-MMM-yyyy");
                }
                objARMachineSwipeData.TotalBatchCount = ARMachineFileDataBatchList.Count;
                objARMachineSwipeData.SUHId = (int)SUHId;
                objARMachineSwipeData.FilePath = objARMachineFile.ExcelFilePath;
                objARMachineSwipeData.ExcelFileName = objARMachineFile.ExcelFileName;
                objARMachineSwipeData.FetchTime = DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss");
                objARMachineSwipeData.LastUpdatedTime = DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss");

                for (int batchCount = 1; batchCount <= ARMachineFileDataBatchList.Count; batchCount++)
                {
                    objARMachineSwipeData.IsLastFile = false;
                    if (batchCount == ARMachineFileDataBatchList.Count && isLastFile)
                    {
                        objARMachineSwipeData.IsLastFile = true;
                    }
                    objARMachineSwipeData.RecordCount = ARMachineFileDataBatchList[batchCount - 1].RecordCount;
                    objARMachineSwipeData.BatchNo = ARMachineFileDataBatchList[batchCount - 1].BatchNo;
                    objARMachineSwipeData.SwipeStartTime = ARMachineFileDataBatchList[batchCount - 1].SwipeStartTime;
                    objARMachineSwipeData.SwipeEndTime = ARMachineFileDataBatchList[batchCount - 1].SwipeEndTime;

                    ARMachineDataSwipeDataBatch objARMachineDataSwipeDataBatch = new ARMachineDataSwipeDataBatch();
                    objARMachineDataSwipeDataBatch.BatchNo = objARMachineSwipeData.BatchNo;
                    objARMachineDataSwipeDataBatch.SwipeDataXml = ARMachineFileDataBatchList[batchCount - 1].SwipeData;

                    OperationStatus objOperationStatus = ObjExternalServiceBL.InsertARMachineData(objARMachineSwipeData, objARMachineDataSwipeDataBatch);


                    if (objARMachineSwipeData.IsLastFile || objARMachineSwipeData.BatchNo == objARMachineSwipeData.TotalBatchCount)
                    {
                        string attendanceStr = objARMachineSwipeData.TenantId.ToString() + ", " + objARMachineSwipeData.LocationId.ToString() + ", " + objARMachineSwipeData.ARMachineId.ToString() + ", " + objARMachineSwipeData.SUHId.ToString() + ", " + objARMachineSwipeData.AttendanceDate + ", " + objARMachineSwipeData.TotalBatchCount.ToString() + ", " + objARMachineSwipeData.BatchNo.ToString();
                        if (objOperationStatus.Status)
                        {
                            status = true;
                            JobLogging("Successfully uploaded. " + attendanceStr);
                            InsertOperationLog(false, "Successfully uploaded. " + attendanceStr, "TO", "", "");
                            TransferFile(objARMachineFile.SuccessFolderPath, objARMachineFile.ExcelFilePath, objARMachineFile.ExcelFileName);
                        }
                        else
                        {
                            status = false;
                            JobLogging("Uploading failed. " + attendanceStr);
                            InsertOperationLog(true, "Uploading failed. " + attendanceStr, "TO", "InsertAttendanceSwipeData()", "");
                            TransferFile(objARMachineFile.ErrorFolderPath, objARMachineFile.ExcelFilePath, objARMachineFile.ExcelFileName);
                            break;
                        }
                    }
                    else
                    {
                        string attendanceStr = objARMachineSwipeData.TenantId.ToString() + ", " + objARMachineSwipeData.LocationId.ToString() + ", " + objARMachineSwipeData.ARMachineId.ToString() + ", " + objARMachineSwipeData.SUHId.ToString() + ", " + objARMachineSwipeData.AttendanceDate + ", " + objARMachineSwipeData.TotalBatchCount.ToString() + ", " + objARMachineSwipeData.BatchNo.ToString();
                        if (objOperationStatus.Status)
                        {
                            objARMachineSwipeData.SUOId = objOperationStatus.OperationId.ToString();
                            status = true;
                            JobLogging("Batch successfully uploaded. " + attendanceStr);
                            InsertOperationLog(false, "Batch successfully uploaded. " + attendanceStr, "TO", "", "");
                        }
                        else
                        {

                            status = false;
                            JobLogging("Batch uploading failed. " + attendanceStr);
                            InsertOperationLog(true, "Batch uploading failed. " + attendanceStr, "TO", "InsertAttendanceSwipeData()", "");
                            TransferFile(objARMachineFile.ErrorFolderPath, objARMachineFile.ExcelFilePath, objARMachineFile.ExcelFileName);
                            break;
                        }
                    }


                }

            }
            catch (Exception ex)
            {
                string message = "Error:InsertAttendanceSwipeData()-" + ex.Message;
                JobLogging(message);
                InsertOperationLog(true, message, "TO", "InsertAttendanceSwipeData()", "");
            }
            return status;
        }


        #endregion

        #region UploadSwipeData2

        private bool UploadSwipeData2()
        {
            bool status = false;
            try
            {
                ARMAchineList = null;
                SUHId = 0;
                IntegratedARMFileList = new List<IntegratedARMFile>();
                DT_AttendanceExcelData = new DataTable();
                ARMachineFileDataBatchList = new List<ARMachineFileData>();
                objIntegratedARMFileGbl = null;
                //if (!ObjExternalServiceBL.GetSchedulerErrorPending())
                if (true)
                {
                    if (ReadARMachineList())
                    {
                        if (ReadFileListFromDisk2())
                        {
                            int countARMListi = 0;
                            if (ARMAchineList != null) countARMListi = ARMAchineList.Length;

                            int countReadFileListi = 0;
                            if (IntegratedARMFileList != null) countReadFileListi = IntegratedARMFileList.Count;

                            InsertOperationLog(false, "ARMachine Count:" + countARMListi.ToString() + " File count:" + countReadFileListi, "RD", "ReadFileListFromDisk2()", "");

                            if (countReadFileListi > 0)
                            {
                                if (InsertSchedulerOperationHeader2())
                                {

                                    for (int armFileCount = 0; armFileCount < IntegratedARMFileList.Count; armFileCount++)
                                    {
                                        DT_AttendanceExcelData = null;
                                        objIntegratedARMFileGbl = IntegratedARMFileList[armFileCount];
                                        if (ReadSwipeDataFromExcel2(objIntegratedARMFileGbl))
                                        {
                                            if (objIntegratedARMFileGbl.IsReadSuccessful)
                                            {
                                                if (ValidateIntegratedFileBOFEOF(objIntegratedARMFileGbl))
                                                {
                                                    if (FormatExcelFile(objIntegratedARMFileGbl))
                                                    {
                                                        if (InsertIntegratedFileHeader(objIntegratedARMFileGbl))
                                                        {
                                                            bool isFileUploaded = true;
                                                            int armCount = 0;
                                                            for (armCount = 0; armCount < ARMAchineList.Length; armCount++)
                                                            {
                                                                if (CheckARMActivationDate(objIntegratedARMFileGbl, ARMAchineList[armCount].ActivationDate))
                                                                {
                                                                    if (SeparateARMExcelData(objIntegratedARMFileGbl, ARMAchineList[armCount].ARMachineId))
                                                                    {
                                                                        ARMachineFileDataBatchList = new List<ARMachineFileData>();
                                                                        if (ValidateExcelSwipeData2(objIntegratedARMFileGbl))
                                                                        {
                                                                            bool isLastFile = false;
                                                                            if (armFileCount == IntegratedARMFileList.Count - 1)
                                                                            {
                                                                                isLastFile = true;
                                                                            }
                                                                            if (!InsertAttendanceSwipeData2(objIntegratedARMFileGbl, isLastFile, ARMAchineList[armCount]))
                                                                            {
                                                                                isFileUploaded = false;
                                                                                break;
                                                                            }
                                                                        }
                                                                        else
                                                                        {
                                                                            isFileUploaded = false;
                                                                            InsertOperationLog(true, objIntegratedARMFileGbl.Remark, "VS", "ValidateExcelSwipeData2()", objIntegratedARMFileGbl.ExcelFilePath);
                                                                            break;
                                                                        }
                                                                    }
                                                                    else
                                                                    {
                                                                        isFileUploaded = false;
                                                                        InsertOperationLog(true, objIntegratedARMFileGbl.Remark, "VS", "SeparateARMExcelData()", objIntegratedARMFileGbl.ExcelFilePath);
                                                                        break;
                                                                    }
                                                                }
                                                            }
                                                            if (isFileUploaded && armCount >= ARMAchineList.Length)
                                                            {
                                                                InsertIntegratedFileUploadingComplete(objIntegratedARMFileGbl);
                                                                TransferFile(objIntegratedARMFileGbl.SuccessFolderPath, objIntegratedARMFileGbl.ExcelFilePath, objIntegratedARMFileGbl.ExcelFileName);
                                                            }

                                                        }
                                                        else
                                                        {
                                                            InsertOperationLog(true, objIntegratedARMFileGbl.Remark, "IF", "InsertIntegratedFileHeader()", "");
                                                        }
                                                    }
                                                    else
                                                    {
                                                        InsertOperationLog(true, objIntegratedARMFileGbl.Remark, "VS", "FormatExcelFile()", objIntegratedARMFileGbl.ExcelFilePath);
                                                    }
                                                }
                                                else
                                                {
                                                    InsertOperationLog(true, objIntegratedARMFileGbl.Remark, "VS", "ValidateIntegratedFileBOFEOF()", objIntegratedARMFileGbl.ExcelFilePath);
                                                }
                                            }
                                        }
                                        else
                                        {
                                            InsertOperationLog(true, objIntegratedARMFileGbl.Remark, "RS", "ReadSwipeDataFromExcel2()", objIntegratedARMFileGbl.ExcelFilePath);
                                        }
                                        objIntegratedARMFileGbl = null;
                                    }
                                    objIntegratedARMFileGbl = null;
                                }
                                else
                                {
                                    int countARMList = 0;
                                    if (ARMAchineList != null) countARMList = ARMAchineList.Length;

                                    int countReadFileList = 0;
                                    if (ARMachineFileList != null) countReadFileList = ARMachineFileList.Count;

                                    InsertOperationLog(true, "Header insertion failed." + countARMList.ToString() + "," + countReadFileList.ToString(), "UH", "InsertSchedulerOperationHeader()", "");
                                }
                            }

                        }
                        else
                        {
                            int countARMList = 0;
                            if (ARMAchineList != null) countARMList = ARMAchineList.Length;

                            int countReadFileList = 0;
                            if (ARMachineFileList != null) countReadFileList = ARMachineFileList.Count;

                            InsertOperationLog(false, "No files found." + countARMList.ToString() + "," + countReadFileList.ToString(), "RD", "ReadFileListFromDisk2()", "");

                        }
                    }
                }
                else
                {
                    InsertOperationLog(false, "Error is pending. So cannot proceed.", "TO", "GetSchedulerErrorPending()", "");
                }
            }
            catch (Exception ex)
            {
                throw new Exception("UploadSwipeData2()-" + ex.Message, ex);
            }
            finally
            {
                objIntegratedARMFileGbl = null;
                ARMAchineList = null;
                ARMachineFileList = null;
                ARMachineFileDataBatchList = null;
                DT_AttendanceExcelData = null;
                DT_IntegratedAttendanceData = null;
            }
            return status;
        }

        private bool CheckARMActivationDate(IntegratedARMFile objARMachineFile, string activationDate)
        {
            bool status = true;
            DateTime activationDateDt = DateTime.Today;
            try
            {
                if (activationDate != null)
                {
                    if (activationDate.Trim().Length > 0)
                    {
                        if (CheckDate(activationDate, ref activationDateDt))
                        {
                            if (objARMachineFile.AttendanceDate.Value < activationDateDt)
                            {
                                status = false;
                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                status = false;
                string message = "Error:CheckARMActivationDate()-" + ex.Message;
                JobLogging(message);
                InsertOperationLog(true, message, "TO", "CheckARMActivationDate()", objARMachineFile.ExcelFilePath);
            }
            return status;
        }

        private bool ReadFileListFromDisk2()
        {
            bool status = false;
            DateTime checkFileDate = DateTime.Today;
            try
            {
                IntegratedARMFileList = new List<IntegratedARMFile>();

                string _DatasourcePath = SCHEDULER_DATASOURCE_PATH;
                int armCount = 0;
                IntegratedARMFile objIntegratedARMFile;
                if (Directory.Exists(_DatasourcePath))
                {
                    IEnumerable<string> excelFileList = Directory.EnumerateFiles(_DatasourcePath);
                    foreach (string excelFilePath in excelFileList)
                    {
                        if (CheckDate(Path.GetFileNameWithoutExtension(excelFilePath), ref checkFileDate))
                        {
                            status = true;
                            objIntegratedARMFile = new IntegratedARMFile();
                            objIntegratedARMFile.SuccessFolderPath = ARMAchineList[armCount].SuccessFolderPath;
                            objIntegratedARMFile.ErrorFolderPath = ARMAchineList[armCount].ErrorFolderPath;
                            objIntegratedARMFile.TableName = ARMAchineList[armCount].TableName;
                            objIntegratedARMFile.EmpIdCol = ARMAchineList[armCount].EmpIdColPos;
                            objIntegratedARMFile.SwipeTimeColumn = ARMAchineList[armCount].SwipeTimeColPos;
                            objIntegratedARMFile.ARMIdColumn = ARMAchineList[armCount].ARMIdColPos;
                            objIntegratedARMFile.ExcelFilePath = excelFilePath;
                            objIntegratedARMFile.LastAttendanceDate = null;
                            objIntegratedARMFile.BatchSize = UPLOADBATCHSIZE;
                            objIntegratedARMFile.BOF = ARMAchineList[armCount].BOF;
                            objIntegratedARMFile.EOF = ARMAchineList[armCount].EOF;
                            if (ARMAchineList[armCount].LastAttendanceDate.Trim().Length > 0)
                            {
                                objIntegratedARMFile.LastAttendanceDate = Convert.ToDateTime(ARMAchineList[armCount].LastAttendanceDate);
                            }
                            IntegratedARMFileList.Add(objIntegratedARMFile);
                        }

                    }

                }

            }
            catch (Exception ex)
            {
                throw new Exception("ReadFileListFromDisk2()-" + ex.Message, ex);
            }
            return status;
        }

        private bool InsertSchedulerOperationHeader2()
        {
            bool status = false;
            try
            {
                OperationHeader objOperationHeader = new OperationHeader();
                objOperationHeader.FileCount = IntegratedARMFileList.Count;
                objOperationHeader.ARMachineCount = ARMAchineList.Length;
                OperationStatus objOperationStatus = ObjExternalServiceBL.InsertSchedulerUploadingHeader(objOperationHeader);
                if (objOperationStatus.Status)
                {
                    SUHId = objOperationStatus.OperationId;
                    status = true;
                }
            }
            catch (Exception ex)
            {
                throw new Exception("InsertSchedulerOperationHeader()-" + ex.Message, ex);
            }
            return status;
        }

        private bool ReadSwipeDataFromExcel2(IntegratedARMFile objARMachineFile)
        {

            DataSet ds = null;
            OleDbConnection oledbConn = new OleDbConnection();
            bool status = false;
            try
            {

                string GenAttendanceExcelConStr = ConfigurationManager.AppSettings["AttendanceExcelConStr"].ToString();
                string excelConStr = GenAttendanceExcelConStr.Replace("EMATTENDANCEFILEPATH", objARMachineFile.ExcelFilePath);
                oledbConn.ConnectionString = excelConStr;
                oledbConn.Open();
                OleDbCommand cmd = new OleDbCommand("SELECT * FROM [" + objARMachineFile.TableName + "$]", oledbConn);
                OleDbDataAdapter oleda = new OleDbDataAdapter();
                oleda.SelectCommand = cmd;
                ds = new DataSet("EMROOT");
                oleda.Fill(ds, "EMATTENDANCE");
                oledbConn.Close();

                if (ds.Tables.Count > 0)
                {
                    DT_AttendanceExcelData = ds.Tables[0];
                    objARMachineFile.IsReadSuccessful = true;
                    status = true;
                    objARMachineFile.ExcelFileName = Path.GetFileName(objARMachineFile.ExcelFilePath);


                    // objARMachineFile.AttendanceDate = Convert.ToDateTime(Path.GetFileNameWithoutExtension(objARMachineFile.ExcelFilePath));

                    string FileNameWithoutExtension = Path.GetFileNameWithoutExtension(objARMachineFile.ExcelFilePath);
                    DateTime fileNameDate = DateTime.Today;
                    if (CheckDate(FileNameWithoutExtension, ref fileNameDate))
                    {
                        objARMachineFile.AttendanceDate = fileNameDate;
                    }

                }
            }
            catch (Exception ex)
            {
                oledbConn.Close();
                oledbConn.Dispose();

                status = false;
                string message = "Error:ReadSwipeDataFromExcel2()-" + ex.Message;
                JobLogging(message);
                InsertOperationLog(true, message, "TO", "ReadSwipeDataFromExcel2()", objARMachineFile.ExcelFilePath);
            }
            finally
            {
                oledbConn.Close();
                oledbConn.Dispose();
            }
            return status;
        }

        private bool ValidateIntegratedFileBOFEOF(IntegratedARMFile objARMachineFile)
        {
            bool status = false;
            try
            {

                string bof = objARMachineFile.BOF.Trim();
                string eof = objARMachineFile.EOF.Trim();
                int? bofRowIndex = null;
                int? eofRowIndex = null;

                int totalRowCount = DT_AttendanceExcelData.Rows.Count;
                if (totalRowCount > 0)
                {
                    if (bof.Length > 0)
                    {
                        for (int count = 0; count < totalRowCount; count++)
                        {
                            if (DT_AttendanceExcelData.Rows[count][0].ToString().Trim() == bof)
                            {
                                bofRowIndex = count;
                                break;
                            }
                        }
                    }

                    if (bofRowIndex.HasValue)
                    {
                        for (int count = 0; count <= bofRowIndex; count++)
                        {
                            DT_AttendanceExcelData.Rows.RemoveAt(0);
                        }
                    }


                    totalRowCount = DT_AttendanceExcelData.Rows.Count;
                    if (eof.Length > 0)
                    {
                        for (int count = totalRowCount - 1; count >= 0; count--)
                        {
                            if (DT_AttendanceExcelData.Rows[count][0].ToString().Trim() == eof)
                            {
                                eofRowIndex = count;
                                break;
                            }
                        }
                    }

                    if (eofRowIndex.HasValue)
                    {
                        for (int count = totalRowCount - 1; count >= eofRowIndex; count--)
                        {
                            DT_AttendanceExcelData.Rows.RemoveAt(count);
                        }
                    }
                }

                if (bofRowIndex.HasValue && eofRowIndex.HasValue)
                {
                    status = true;
                    objARMachineFile.FileRecordCount = DT_AttendanceExcelData.Rows.Count;
                }
                else
                {
                    objARMachineFile.Remark = "File BOF or EOF is not found!!";
                }


            }
            catch (Exception ex)
            {
                status = false;
                string message = "Error:ValidateIntegratedFileBOFEOF()-" + ex.Message;
                JobLogging(message);
                InsertOperationLog(true, message, "TO", "ValidateIntegratedFileBOFEOF()", objARMachineFile.ExcelFilePath);
            }
            return status;
        }

        private bool FormatExcelFile(IntegratedARMFile objARMachineFile)
        {
            bool status = false;
            bool isFormat = false;
            bool isEmpId = true;
            bool isSwipeTime = true;
            bool isARM = true;

            try
            {
                DT_IntegratedAttendanceData = DT_AttendanceExcelData;

                int colCount = DT_IntegratedAttendanceData.Columns.Count;
                if (colCount > objARMachineFile.EmpIdCol && colCount > objARMachineFile.SwipeTimeColumn && colCount > objARMachineFile.ARMIdColumn)
                {
                    DT_IntegratedAttendanceData.Columns[objARMachineFile.EmpIdCol].ColumnName = "EmpId";
                    DT_IntegratedAttendanceData.Columns[objARMachineFile.SwipeTimeColumn].ColumnName = "SwipeTime";
                    DT_IntegratedAttendanceData.Columns[objARMachineFile.ARMIdColumn].ColumnName = "ARMId";
                    DT_AttendanceExcelData = null;

                    isFormat = true;
                }
                else
                {
                    objARMachineFile.Remark = "File format is not correct!!";
                }

                if (isFormat)
                {
                    DataTable dtAttendance = GenerateAttendanceIntTable();
                    DataRow dr;
                    int rowCount = 0;
                    for (rowCount = 0; rowCount < DT_IntegratedAttendanceData.Rows.Count; rowCount++)
                    {
                        if (DT_IntegratedAttendanceData.Rows[rowCount][objARMachineFile.EmpIdCol].ToString().Trim().Length == 0)
                        {
                            isEmpId = false;
                            objARMachineFile.Remark = "AR-Empid coumn canot be blank!!";
                        }

                        if (DT_IntegratedAttendanceData.Rows[rowCount][objARMachineFile.SwipeTimeColumn].ToString().Trim().Length == 0)
                        {
                            isSwipeTime = false;
                            objARMachineFile.Remark = "Swipetime coumn canot be blank!!";
                        }

                        if (DT_IntegratedAttendanceData.Rows[rowCount][objARMachineFile.ARMIdColumn].ToString().Trim().Length == 0)
                        {
                            isARM = false;
                            objARMachineFile.Remark = "ARMId coumn canot be blank!!";
                        }

                        if (isEmpId && isSwipeTime && isARM)
                        {
                            dr = dtAttendance.NewRow();
                            dr["AR_Emp_Id"] = DT_IntegratedAttendanceData.Rows[rowCount][objARMachineFile.EmpIdCol].ToString().Trim();
                            dr["Swipe_DateTime"] = DT_IntegratedAttendanceData.Rows[rowCount][objARMachineFile.SwipeTimeColumn].ToString().Trim();
                            dr["AR_Id"] = DT_IntegratedAttendanceData.Rows[rowCount][objARMachineFile.ARMIdColumn].ToString().Trim();
                            dtAttendance.Rows.Add(dr);
                        }
                        else
                        {
                            break;
                        }
                    }

                    if (rowCount == DT_IntegratedAttendanceData.Rows.Count && isEmpId && isSwipeTime && isARM)
                    {
                        status = true;
                        DT_IntegratedAttendanceData = dtAttendance;
                    }

                }


                //if (isFormat)
                //{
                //    DataRow[] drEmp = DT_IntegratedAttendanceData.Select("EmpId=''");
                //    if (drEmp.Length == 0)
                //    {
                //        DataRow[] drSwipe = DT_IntegratedAttendanceData.Select("SwipeTime=''");
                //        if (drSwipe.Length == 0)
                //        {
                //            DataRow[] drARM = DT_IntegratedAttendanceData.Select("ARMId=''");
                //            if (drARM.Length == 0)
                //            {
                //                status = true;
                //            }
                //            else
                //            {
                //                objARMachineFile.Remark = "ARMId coumn canot be blank!!";
                //            }
                //        }
                //        else
                //        {
                //            objARMachineFile.Remark = "Swipetime coumn canot be blank!!";
                //        }
                //    }
                //    else
                //    {
                //        objARMachineFile.Remark = "AR-Empid coumn canot be blank!!";
                //    }
                //}


            }
            catch (Exception ex)
            {
                status = false;
                string message = "Error:FormatExcelFile()-" + ex.Message;
                JobLogging(message);
                InsertOperationLog(true, message, "TO", "FormatExcelFile()", objARMachineFile.ExcelFilePath);
            }
            return status;
        }

        private bool SeparateARMExcelData(IntegratedARMFile objARMachineFile, int armId)
        {
            bool status = true;
            try
            {
                DT_AttendanceExcelData = null;
                DT_IntegratedAttendanceData.DefaultView.RowFilter = "";
                DT_IntegratedAttendanceData.DefaultView.RowFilter = "AR_Id='" + armId.ToString() + "'";
                DT_AttendanceExcelData = DT_IntegratedAttendanceData.DefaultView.ToTable();
                DT_IntegratedAttendanceData.DefaultView.RowFilter = "";


            }
            catch (Exception ex)
            {
                status = false;
                string message = "Error:SeparateARMExcelData()-" + ex.Message;
                JobLogging(message);
                InsertOperationLog(true, message, "TO", "SeparateARMExcelData()", objARMachineFile.ExcelFilePath);
            }
            return status;
        }

        private bool ValidateExcelSwipeData2(IntegratedARMFile objARMachineFile)
        {
            bool status = true;
            try
            {
                bool isErrorFound = false;
                string arEmpId = "";
                string swipeDateTime = "";
                DateTime swipeDateTimeDate = DateTime.Now;
                DataRow dr;
                DataTable dtAttendance = GenerateAttendanceTable();
                ARMachineFileDataBatchList = new List<ARMachineFileData>();

                if (IsSwipeDataPresent2(objARMachineFile))
                {

                    decimal dtRowCount = DT_AttendanceExcelData.Rows.Count;
                    decimal decBatchCount = dtRowCount / (decimal)objARMachineFile.BatchSize;
                    int maxBatchCount = (int)decBatchCount;
                    if (maxBatchCount < decBatchCount)
                    {
                        maxBatchCount++;
                    }

                    List<int> batchList = new List<int>();
                    for (int batchCount = 0; batchCount < maxBatchCount; batchCount++)
                    {
                        batchList.Add(objARMachineFile.BatchSize * batchCount);
                    }

                    int batchIndex = 0;

                    for (int rowCount = 0; rowCount < DT_AttendanceExcelData.Rows.Count; rowCount++)
                    {
                        if (rowCount == batchList[batchIndex])
                        {
                            dtAttendance = GenerateAttendanceTable();
                        }
                        arEmpId = DT_AttendanceExcelData.Rows[rowCount][objARMachineFile.EmpIdCol].ToString();
                        swipeDateTime = DT_AttendanceExcelData.Rows[rowCount][objARMachineFile.SwipeTimeColumn].ToString();
                        if (arEmpId.Trim().Length > 0)
                        {
                            if (CheckDate(swipeDateTime, ref swipeDateTimeDate))
                            {
                                if (isErrorFound == false)
                                {

                                    dr = dtAttendance.NewRow();
                                    dr["AR_Emp_Id"] = arEmpId;
                                    dr["Swipe_DateTime"] = swipeDateTimeDate.ToString("dd-MMM-yyyy HH:mm:ss");
                                    dr["Swipe_DateTime_Date"] = swipeDateTimeDate;
                                    dtAttendance.Rows.Add(dr);

                                }
                            }
                            else
                            {
                                isErrorFound = true;
                                status = false;
                                objARMachineFile.IsReadSuccessful = false;
                                objARMachineFile.Remark = "Invalid datetime.";
                                break;

                            }
                        }
                        if (batchIndex < maxBatchCount - 1)
                        {
                            if (rowCount == batchList[batchIndex + 1] - 1)
                            {
                                if (dtAttendance.Rows.Count > 0)
                                {
                                    ARMachineFileDataBatchList.Add(ConvertSwipeDataTableToXML(dtAttendance, batchIndex + 1));
                                }
                                batchIndex++;
                            }
                        }
                        else
                        {
                            if (rowCount == DT_AttendanceExcelData.Rows.Count - 1 && dtAttendance.Rows.Count > 0)
                            {

                                ARMachineFileDataBatchList.Add(ConvertSwipeDataTableToXML(dtAttendance, batchIndex + 1));
                                batchIndex++;
                            }
                        }


                    }
                }
                else
                {
                    dtAttendance = GenerateAttendanceTable();
                    ARMachineFileDataBatchList.Add(ConvertSwipeDataTableToXML(dtAttendance, 1));
                }



            }
            catch (Exception ex)
            {
                status = false;
                string message = "Error:ValidateExcelSwipeData2()-" + ex.Message;
                JobLogging(message);
                InsertOperationLog(true, message, "TO", "ValidateExcelSwipeData2()", objARMachineFile.ExcelFilePath);
            }
            return status;
        }

        private DataTable GenerateAttendanceIntTable()
        {
            DataTable dtAttendance = new DataTable();
            try
            {
                dtAttendance.Columns.Add(new DataColumn("AR_Emp_Id", System.Type.GetType("System.String")));
                dtAttendance.Columns.Add(new DataColumn("Swipe_DateTime", System.Type.GetType("System.String")));
                dtAttendance.Columns.Add(new DataColumn("AR_Id", System.Type.GetType("System.String")));
            }
            catch (Exception ex)
            {
                throw new Exception("GenerateAttendanceIntTable()-" + ex.Message, ex);
            }

            return dtAttendance;
        }

        private bool IsSwipeDataPresent2(IntegratedARMFile objARMachineFile)
        {
            bool status = false;
            try
            {
                if (DT_AttendanceExcelData.Columns.Count >= 3)
                {
                    if (DT_AttendanceExcelData.Rows.Count > 0)
                    {
                        string colEmp = DT_AttendanceExcelData.Columns[objARMachineFile.EmpIdCol].ColumnName;
                        DT_AttendanceExcelData.DefaultView.RowFilter = colEmp + "<>''";
                        if (DT_AttendanceExcelData.DefaultView.Count > 0)
                        {
                            status = true;
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                throw new Exception("IsSwipeDataPresent2()-" + ex.Message, ex);
            }
            return status;
        }

        private bool InsertIntegratedFileHeader(IntegratedARMFile objARMachineFile)
        {
            bool status = false;
            try
            {
                SFUId = 0;
                OperationFileHeader objOperationFileHeader = new OperationFileHeader();
                objOperationFileHeader.SUHId = Convert.ToInt16(SUHId);
                objOperationFileHeader.FileName = objARMachineFile.ExcelFileName;
                objOperationFileHeader.AttendanceDate = objARMachineFile.AttendanceDate.Value.ToString("dd-MMM-yyyy");
                objOperationFileHeader.FileRecordCount = objARMachineFile.FileRecordCount;
                OperationStatus objOperationStatus = ObjExternalServiceBL.InsertSchedulerFileHeader(objOperationFileHeader);
                if (objOperationStatus.Status)
                {
                    SFUId = objOperationStatus.OperationId;
                    objARMachineFile.SFUId = SFUId;
                    status = true;
                }
            }
            catch (Exception ex)
            {
                status = false;
                string message = "Error:InsertIntegratedFileHeader()-" + ex.Message;
                JobLogging(message);
                InsertOperationLog(true, message, "TO", "InsertIntegratedFileHeader()", objARMachineFile.ExcelFilePath);
            }
            return status;
        }

        private bool InsertIntegratedFileUploadingComplete(IntegratedARMFile objARMachineFile)
        {
            bool status = false;
            try
            {
                OperationFileHeaderComplete objOperationFileHeaderComplete = new OperationFileHeaderComplete();
                objOperationFileHeaderComplete.SUHId = Convert.ToInt16(SUHId);
                objOperationFileHeaderComplete.SFUId = SFUId;
                OperationStatus objOperationStatus = ObjExternalServiceBL.UpdateSchedulerFileUploadingComplete(objOperationFileHeaderComplete);
                if (objOperationStatus.Status)
                {
                    status = true;
                }
            }
            catch (Exception ex)
            {
                status = false;
                string message = "Error:InsertIntegratedFileUploadingComplete()-" + ex.Message;
                JobLogging(message);
                InsertOperationLog(true, message, "TO", "InsertIntegratedFileUploadingComplete()", objARMachineFile.ExcelFilePath);

            }
            return status;
        }

        private bool InsertAttendanceSwipeData2(IntegratedARMFile objARMachineFile, bool isLastFile, ARMachineDetails objARMachine)
        {
            bool status = false;
            try
            {
                ARMachineSwipeData objARMachineSwipeData = new ARMachineSwipeData();
                objARMachineSwipeData.TenantId = objARMachine.TenantId;
                objARMachineSwipeData.LocationId = objARMachine.LocationId;
                objARMachineSwipeData.ARMachineId = objARMachine.ARMachineId;
                objARMachineSwipeData.AttendanceDate = "";
                if (objARMachineFile.AttendanceDate.HasValue)
                {
                    objARMachineSwipeData.AttendanceDate = objARMachineFile.AttendanceDate.Value.ToString("dd-MMM-yyyy");
                }
                objARMachineSwipeData.TotalBatchCount = ARMachineFileDataBatchList.Count;
                objARMachineSwipeData.SUHId = (int)SUHId;
                objARMachineSwipeData.FilePath = objARMachineFile.ExcelFilePath;
                objARMachineSwipeData.ExcelFileName = objARMachineFile.ExcelFileName;
                objARMachineSwipeData.FetchTime = DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss");
                objARMachineSwipeData.LastUpdatedTime = DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss");
                objARMachineSwipeData.SFUId = SFUId.ToString();

                for (int batchCount = 1; batchCount <= ARMachineFileDataBatchList.Count; batchCount++)
                {
                    objARMachineSwipeData.IsLastFile = false;
                    if (batchCount == ARMachineFileDataBatchList.Count && isLastFile)
                    {
                        objARMachineSwipeData.IsLastFile = true;
                    }
                    objARMachineSwipeData.RecordCount = ARMachineFileDataBatchList[batchCount - 1].RecordCount;
                    objARMachineSwipeData.BatchNo = ARMachineFileDataBatchList[batchCount - 1].BatchNo;
                    objARMachineSwipeData.SwipeStartTime = ARMachineFileDataBatchList[batchCount - 1].SwipeStartTime;
                    objARMachineSwipeData.SwipeEndTime = ARMachineFileDataBatchList[batchCount - 1].SwipeEndTime;

                    ARMachineDataSwipeDataBatch objARMachineDataSwipeDataBatch = new ARMachineDataSwipeDataBatch();
                    objARMachineDataSwipeDataBatch.BatchNo = objARMachineSwipeData.BatchNo;
                    objARMachineDataSwipeDataBatch.SwipeDataXml = ARMachineFileDataBatchList[batchCount - 1].SwipeData;

                    OperationStatus objOperationStatus = ObjExternalServiceBL.InsertARMachineData2(objARMachineSwipeData, objARMachineDataSwipeDataBatch);


                    if (objARMachineSwipeData.IsLastFile || objARMachineSwipeData.BatchNo == objARMachineSwipeData.TotalBatchCount)
                    {
                        string attendanceStr = objARMachineSwipeData.TenantId.ToString() + ", " + objARMachineSwipeData.LocationId.ToString() + ", " + objARMachineSwipeData.ARMachineId.ToString() + ", " + objARMachineSwipeData.SUHId.ToString() + ", " + objARMachineSwipeData.AttendanceDate + ", " + objARMachineSwipeData.TotalBatchCount.ToString() + ", " + objARMachineSwipeData.BatchNo.ToString();
                        if (objOperationStatus.Status)
                        {
                            status = true;
                            JobLogging("Successfully uploaded. " + attendanceStr);
                            InsertOperationLog(false, "Successfully uploaded. " + attendanceStr, "SU", "", "");
                            //TransferFile(objARMachineFile.SuccessFolderPath, objARMachineFile.ExcelFilePath, objARMachineFile.ExcelFileName);
                        }
                        else
                        {
                            status = false;
                            JobLogging("Uploading failed. " + attendanceStr);
                            InsertOperationLog(true, "Uploading failed. " + attendanceStr, "SU", "InsertAttendanceSwipeData2()", "");
                            TransferFile(objARMachineFile.ErrorFolderPath, objARMachineFile.ExcelFilePath, objARMachineFile.ExcelFileName);
                            break;
                        }
                    }
                    else
                    {
                        string attendanceStr = objARMachineSwipeData.TenantId.ToString() + ", " + objARMachineSwipeData.LocationId.ToString() + ", " + objARMachineSwipeData.ARMachineId.ToString() + ", " + objARMachineSwipeData.SUHId.ToString() + ", " + objARMachineSwipeData.AttendanceDate + ", " + objARMachineSwipeData.TotalBatchCount.ToString() + ", " + objARMachineSwipeData.BatchNo.ToString();
                        if (objOperationStatus.Status)
                        {
                            objARMachineSwipeData.SUOId = objOperationStatus.OperationId.ToString();
                            status = true;
                            JobLogging("Batch successfully uploaded. " + attendanceStr);
                            InsertOperationLog(false, "Batch successfully uploaded. " + attendanceStr, "SU", "", "");
                        }
                        else
                        {

                            status = false;
                            JobLogging("Batch uploading failed. " + attendanceStr);
                            InsertOperationLog(true, "Batch uploading failed. " + attendanceStr, "SU", "InsertAttendanceSwipeData2()", "");
                            TransferFile(objARMachineFile.ErrorFolderPath, objARMachineFile.ExcelFilePath, objARMachineFile.ExcelFileName);
                            break;
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                string message = "Error:InsertAttendanceSwipeData2()-" + ex.Message;
                JobLogging(message);
                InsertOperationLog(true, message, "TO", "InsertAttendanceSwipeData2()", "");
            }
            return status;
        }



        #endregion

        #region UploadSwipeData3

        private bool CheckFileExists3()
        {
            bool status = false;
            DateTime checkFileDate = DateTime.Today;
            try
            {
                if (SchedulerFileList == null)
                {
                    ReadSchedulerFileList();
                }

                if (SchedulerFileList != null)
                {
                    for (int fileCount = 0; fileCount < SchedulerFileList.Length; fileCount++)
                    {
                        string datasourcePath = SchedulerFileList[fileCount].DatasourcePath;
                        if (Directory.Exists(datasourcePath))
                        {
                            IEnumerable<string> excelFileList = Directory.EnumerateFiles(datasourcePath);
                            foreach (string excelFilePath in excelFileList)
                            {
                                if (CheckDate(Path.GetFileNameWithoutExtension(excelFilePath), ref checkFileDate))
                                {
                                    status = true;
                                    break;
                                }
                            }

                            if (status) break;

                        }
                    }
                }


            }
            catch (Exception ex)
            {
                throw new Exception("CheckFileExists3()-" + ex.Message, ex);
            }
            return status;
        }

        //private bool ReadSwipaDataFromCSVFile3(IntegratedARMFile objARMachineFile, SchedulerFileDetails objSchedulerFile)
        //{
        //    bool status = false;
        //    try
        //    {
        //        DT_AttendanceExcelData = new DataTable();
        //        if (File.Exists(objARMachineFile.ExcelFilePath))
        //        {


        //            using (TextFieldParser csvReader = new TextFieldParser(objARMachineFile.ExcelFilePath))
        //            {
        //                csvReader.SetDelimiters(new string[] { "," });
        //                csvReader.HasFieldsEnclosedInQuotes = true;

        //                //Read columns from CSV file, remove this line if columns not exits  
        //                string[] colFields = csvReader.ReadFields();

        //                foreach (string column in colFields)
        //                {
        //                    DataColumn datecolumn = new DataColumn(column);
        //                    datecolumn.AllowDBNull = true;
        //                    DT_AttendanceExcelData.Columns.Add(datecolumn);
        //                }

        //                while (!csvReader.EndOfData)
        //                {
        //                    string[] fieldData = csvReader.ReadFields();
        //                    //Making empty value as null
        //                    for (int i = 0; i < fieldData.Length; i++)
        //                    {
        //                        if (fieldData[i] == "")
        //                        {
        //                            fieldData[i] = null;
        //                        }
        //                    }
        //                    DT_AttendanceExcelData.Rows.Add(fieldData);
        //                }

        //            }
        //        }

        //        if (DT_AttendanceExcelData.Rows.Count > 0)
        //        {
        //            objARMachineFile.IsReadSuccessful = true;
        //            status = true;
        //            objARMachineFile.ExcelFileName = Path.GetFileName(objARMachineFile.ExcelFilePath);


        //            // objARMachineFile.AttendanceDate = Convert.ToDateTime(Path.GetFileNameWithoutExtension(objARMachineFile.ExcelFilePath));

        //            string FileNameWithoutExtension = Path.GetFileNameWithoutExtension(objARMachineFile.ExcelFilePath);
        //            DateTime fileNameDate = DateTime.Today;
        //            if (CheckDate(FileNameWithoutExtension, ref fileNameDate))
        //            {
        //                objARMachineFile.AttendanceDate = fileNameDate;
        //            }
        //        }
        //        else
        //        {
        //            if (objSchedulerFile.RowStartPos == -1)
        //            {
        //                objARMachineFile.IsReadSuccessful = true;
        //                status = true;
        //                objARMachineFile.ExcelFileName = Path.GetFileName(objARMachineFile.ExcelFilePath);


        //                // objARMachineFile.AttendanceDate = Convert.ToDateTime(Path.GetFileNameWithoutExtension(objARMachineFile.ExcelFilePath));

        //                string FileNameWithoutExtension = Path.GetFileNameWithoutExtension(objARMachineFile.ExcelFilePath);
        //                DateTime fileNameDate = DateTime.Today;
        //                if (CheckDate(FileNameWithoutExtension, ref fileNameDate))
        //                {
        //                    objARMachineFile.AttendanceDate = fileNameDate;
        //                }
        //            }
        //        }

        //    }
        //    catch (Exception ex)
        //    {

        //        status = false;
        //        string message = "Error:ReadSwipaDataFromCSVFile3()-" + ex.Message;
        //        JobLogging(message);
        //        InsertOperationLog(true, message, "TO", "ReadSwipaDataFromCSVFile3()", objARMachineFile.ExcelFilePath);
        //    }

        //    return status;
        //}

        private bool ReadSwipaDataFromCSVFile3(IntegratedARMFile objARMachineFile, SchedulerFileDetails objSchedulerFile)
        {
            bool status = false;
            try
            {
                DT_AttendanceExcelData = new DataTable();
                if (File.Exists(objARMachineFile.ExcelFilePath))
                {


                    using (TextFieldParser csvReader = new TextFieldParser(objARMachineFile.ExcelFilePath))
                    {
                        csvReader.SetDelimiters(new string[] { "," });
                        csvReader.HasFieldsEnclosedInQuotes = true;

                        //Read columns from CSV file, remove this line if columns not exits  
                        string[] colFields = csvReader.ReadFields();
                        if (objSchedulerFile.RowStartPos == -2 || TEMP_ROW_START_POS_3 == -2)
                        {

                            for (int colCount = 0; colCount < colFields.Length; colCount++)
                            {
                                DataColumn datecolumn = new DataColumn(colCount.ToString());
                                datecolumn.AllowDBNull = true;
                                DT_AttendanceExcelData.Columns.Add(datecolumn);
                            }

                            DataRow drNew = DT_AttendanceExcelData.NewRow();
                            for (int colCount = 0; colCount < colFields.Length; colCount++)
                            {
                                drNew[colCount] = colFields[colCount].ToString();
                            }
                            DT_AttendanceExcelData.Rows.Add(drNew);

                            while (!csvReader.EndOfData)
                            {
                                string[] fieldData = csvReader.ReadFields();
                                //Making empty value as null
                                for (int i = 0; i < fieldData.Length; i++)
                                {
                                    if (fieldData[i] == "")
                                    {
                                        fieldData[i] = null;
                                    }
                                }
                                DT_AttendanceExcelData.Rows.Add(fieldData);
                            }

                            if (objSchedulerFile.RowStartPos == -2)
                            {
                                TEMP_ROW_START_POS_3 = objSchedulerFile.RowStartPos;
                            }
                            objSchedulerFile.RowStartPos = 0;
                        }
                        else
                        {
                            foreach (string column in colFields)
                            {
                                DataColumn datecolumn = new DataColumn(column);
                                datecolumn.AllowDBNull = true;
                                DT_AttendanceExcelData.Columns.Add(datecolumn);
                            }

                            while (!csvReader.EndOfData)
                            {
                                string[] fieldData = csvReader.ReadFields();
                                //Making empty value as null
                                for (int i = 0; i < fieldData.Length; i++)
                                {
                                    if (fieldData[i] == "")
                                    {
                                        fieldData[i] = null;
                                    }
                                }
                                DT_AttendanceExcelData.Rows.Add(fieldData);
                            }
                        }

                    }
                }

                if (DT_AttendanceExcelData.Rows.Count > 0)
                {
                    objARMachineFile.IsReadSuccessful = true;
                    status = true;
                    objARMachineFile.ExcelFileName = Path.GetFileName(objARMachineFile.ExcelFilePath);


                    // objARMachineFile.AttendanceDate = Convert.ToDateTime(Path.GetFileNameWithoutExtension(objARMachineFile.ExcelFilePath));

                    string FileNameWithoutExtension = Path.GetFileNameWithoutExtension(objARMachineFile.ExcelFilePath);
                    DateTime fileNameDate = DateTime.Today;
                    if (CheckDate(FileNameWithoutExtension, ref fileNameDate))
                    {
                        objARMachineFile.AttendanceDate = fileNameDate;
                    }
                }
                else
                {
                    if (objSchedulerFile.RowStartPos == -1)
                    {
                        objARMachineFile.IsReadSuccessful = true;
                        status = true;
                        objARMachineFile.ExcelFileName = Path.GetFileName(objARMachineFile.ExcelFilePath);


                        // objARMachineFile.AttendanceDate = Convert.ToDateTime(Path.GetFileNameWithoutExtension(objARMachineFile.ExcelFilePath));

                        string FileNameWithoutExtension = Path.GetFileNameWithoutExtension(objARMachineFile.ExcelFilePath);
                        DateTime fileNameDate = DateTime.Today;
                        if (CheckDate(FileNameWithoutExtension, ref fileNameDate))
                        {
                            objARMachineFile.AttendanceDate = fileNameDate;
                        }
                    }
                }

            }
            catch (Exception ex)
            {

                status = false;
                string message = "Error:ReadSwipaDataFromCSVFile3()-" + ex.Message;
                JobLogging(message);
                InsertOperationLog(true, message, "TO", "ReadSwipaDataFromCSVFile3()", objARMachineFile.ExcelFilePath);
            }

            return status;
        }