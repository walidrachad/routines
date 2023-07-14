package ma.fm6education.sga.exchange.jobs;

import routines.Numeric;
import routines.TalendString;
import routines.TalendDate;
import routines.system.*;
import routines.system.api.*;

import java.util.List;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;


public class TgrInsertJob implements TalendJob {
    public static String url;
    public static String fileUrl;

    public static String username;

    public static String password;

    public static String port;

    public static String name;

    protected static void logIgnoredError(String message, Throwable cause) {
        System.err.println(message);
        if (cause != null) {
            cause.printStackTrace();
        }

    }


    public final Object obj = new Object();

    // for transmiting parameters purpose
    private Object valueObject = null;

    public Object getValueObject() {
        return this.valueObject;
    }

    public void setValueObject(Object valueObject) {
        this.valueObject = valueObject;
    }

    private final static String defaultCharset = java.nio.charset.Charset.defaultCharset().name();


    private final static String utf8Charset = "UTF-8";

    //contains type for every context property
    public class PropertiesWithType extends java.util.Properties {
        private static final long serialVersionUID = 1L;
        private java.util.Map<String, String> propertyTypes = new java.util.HashMap<>();

        public PropertiesWithType(java.util.Properties properties) {
            super(properties);
        }

        public PropertiesWithType() {
            super();
        }

        public void setContextType(String key, String type) {
            propertyTypes.put(key, type);
        }

        public String getContextType(String key) {
            return propertyTypes.get(key);
        }
    }

    // create and load default properties
    private java.util.Properties defaultProps = new java.util.Properties();

    // create application properties with default
    public class ContextProperties extends PropertiesWithType {

        private static final long serialVersionUID = 1L;

        public ContextProperties(java.util.Properties properties) {
            super(properties);
        }

        public ContextProperties() {
            super();
        }

        public void synchronizeContext() {

        }

    }

    protected ContextProperties context = new ContextProperties(); // will be instanciated by MS.

    public ContextProperties getContext() {
        return this.context;
    }

    private final String jobVersion = "0.1";
    private final String jobName = "TGR_INSERT_JAN23";
    private final String projectName = "FM6";
    public Integer errorCode = null;
    private String currentComponent = "";

    private final java.util.Map<String, Object> globalMap = new java.util.HashMap<String, Object>();
    private final static java.util.Map<String, Object> junitGlobalMap = new java.util.HashMap<String, Object>();

    private final java.util.Map<String, Long> start_Hash = new java.util.HashMap<String, Long>();
    private final java.util.Map<String, Long> end_Hash = new java.util.HashMap<String, Long>();
    private final java.util.Map<String, Boolean> ok_Hash = new java.util.HashMap<String, Boolean>();
    public final List<String[]> globalBuffer = new java.util.ArrayList<String[]>();


    private RunStat runStat = new RunStat();

    // OSGi DataSource
    private final static String KEY_DB_DATASOURCES = "KEY_DB_DATASOURCES";

    private final static String KEY_DB_DATASOURCES_RAW = "KEY_DB_DATASOURCES_RAW";

    public void setDataSources(java.util.Map<String, javax.sql.DataSource> dataSources) {
        java.util.Map<String, TalendDataSource> talendDataSources = new java.util.HashMap<String, TalendDataSource>();
        for (java.util.Map.Entry<String, javax.sql.DataSource> dataSourceEntry : dataSources.entrySet()) {
            talendDataSources.put(dataSourceEntry.getKey(), new TalendDataSource(dataSourceEntry.getValue()));
        }
        globalMap.put(KEY_DB_DATASOURCES, talendDataSources);
        globalMap.put(KEY_DB_DATASOURCES_RAW, new java.util.HashMap<String, javax.sql.DataSource>(dataSources));
    }


    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final java.io.PrintStream errorMessagePS = new java.io.PrintStream(new java.io.BufferedOutputStream(baos));

    public String getExceptionStackTrace() {
        if ("failure".equals(this.getStatus())) {
            errorMessagePS.flush();
            return baos.toString();
        }
        return null;
    }

    private Exception exception;

    public Exception getException() {
        if ("failure".equals(this.getStatus())) {
            return this.exception;
        }
        return null;
    }

    private class TalendException extends Exception {

        private static final long serialVersionUID = 1L;

        private java.util.Map<String, Object> globalMap = null;
        private Exception e = null;
        private String currentComponent = null;
        private String virtualComponentName = null;

        public void setVirtualComponentName(String virtualComponentName) {
            this.virtualComponentName = virtualComponentName;
        }

        private TalendException(Exception e, String errorComponent, final java.util.Map<String, Object> globalMap) {
            this.currentComponent = errorComponent;
            this.globalMap = globalMap;
            this.e = e;
        }

        public Exception getException() {
            return this.e;
        }

        public String getCurrentComponent() {
            return this.currentComponent;
        }


        public String getExceptionCauseMessage(Exception e) {
            Throwable cause = e;
            String message = null;
            int i = 10;
            while (null != cause && 0 < i--) {
                message = cause.getMessage();
                if (null == message) {
                    cause = cause.getCause();
                } else {
                    break;
                }
            }
            if (null == message) {
                message = e.getClass().getName();
            }
            return message;
        }

        @Override
        public void printStackTrace() {
            if (!(e instanceof TalendException || e instanceof TDieException)) {
                if (virtualComponentName != null && currentComponent.indexOf(virtualComponentName + "_") == 0) {
                    globalMap.put(virtualComponentName + "_ERROR_MESSAGE", getExceptionCauseMessage(e));
                }
                globalMap.put(currentComponent + "_ERROR_MESSAGE", getExceptionCauseMessage(e));
                System.err.println("Exception in component " + currentComponent + " (" + jobName + ")");
            }
            if (!(e instanceof TDieException)) {
                if (e instanceof TalendException) {
                    e.printStackTrace();
                } else {
                    e.printStackTrace();
                    e.printStackTrace(errorMessagePS);
                    TgrInsertJob.this.exception = e;
                }
            }
            if (!(e instanceof TalendException)) {
                try {
                    for (java.lang.reflect.Method m : this.getClass().getEnclosingClass().getMethods()) {
                        if (m.getName().compareTo(currentComponent + "_error") == 0) {
                            m.invoke(TgrInsertJob.this, new Object[]{e, currentComponent, globalMap});
                            break;
                        }
                    }

                    if (!(e instanceof TDieException)) {
                    }
                } catch (Exception e) {
                    this.e.printStackTrace();
                }
            }
        }
    }

    public void tFileInputDelimited_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

        end_Hash.put(errorComponent, System.currentTimeMillis());

        status = "failure";

        tFileInputDelimited_1_onSubJobError(exception, errorComponent, globalMap);
    }

    public void tMap_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

        end_Hash.put(errorComponent, System.currentTimeMillis());

        status = "failure";

        tFileInputDelimited_1_onSubJobError(exception, errorComponent, globalMap);
    }

    public void tDBOutput_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

        end_Hash.put(errorComponent, System.currentTimeMillis());

        status = "failure";

        tFileInputDelimited_1_onSubJobError(exception, errorComponent, globalMap);
    }

    public void tFileInputDelimited_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

        resumeUtil.addLog("SYSTEM_LOG", "NODE:" + errorComponent, "", Thread.currentThread().getId() + "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception), "");

    }


    public static class insert_tgr_janv_23Struct implements IPersistableRow<insert_tgr_janv_23Struct> {
        final static byte[] commonByteArrayLock_FM6_TGR_INSERT_JAN23 = new byte[0];
        static byte[] commonByteArray_FM6_TGR_INSERT_JAN23 = new byte[0];
        protected static final int DEFAULT_HASHCODE = 1;
        protected static final int PRIME = 31;
        protected int hashCode = DEFAULT_HASHCODE;
        public boolean hashCodeDirty = true;

        public String loopKey;


        public long ID;

        public long getID() {
            return this.ID;
        }

        public String NUM_PPR;

        public String getNUM_PPR() {
            return this.NUM_PPR;
        }

        public Float MONTANT_COTISATION;

        public Float getMONTANT_COTISATION() {
            return this.MONTANT_COTISATION;
        }

        public String TYPE_COTISATION;

        public String getTYPE_COTISATION() {
            return this.TYPE_COTISATION;
        }

        public String COTISATION_CALCULER;

        public String getCOTISATION_CALCULER() {
            return this.COTISATION_CALCULER;
        }

        public byte CONTROL;

        public byte getCONTROL() {
            return this.CONTROL;
        }

        public Long ECHANGE_MAJ_FILE_ID;

        public Long getECHANGE_MAJ_FILE_ID() {
            return this.ECHANGE_MAJ_FILE_ID;
        }

        public String ECHANGE_MAJ_ID;

        public String getECHANGE_MAJ_ID() {
            return this.ECHANGE_MAJ_ID;
        }

        public String SOURCE_COTISATION_ID;

        public String getSOURCE_COTISATION_ID() {
            return this.SOURCE_COTISATION_ID;
        }


        @Override
        public int hashCode() {
            if (this.hashCodeDirty) {
                final int prime = PRIME;
                int result = DEFAULT_HASHCODE;

                result = prime * result + (int) this.ID;

                this.hashCode = result;
                this.hashCodeDirty = false;
            }
            return this.hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            final insert_tgr_janv_23Struct other = (insert_tgr_janv_23Struct) obj;

            if (this.ID != other.ID)
                return false;


            return true;
        }

        public void copyDataTo(insert_tgr_janv_23Struct other) {

            other.ID = this.ID;
            other.NUM_PPR = this.NUM_PPR;
            other.MONTANT_COTISATION = this.MONTANT_COTISATION;
            other.TYPE_COTISATION = this.TYPE_COTISATION;
            other.COTISATION_CALCULER = this.COTISATION_CALCULER;
            other.CONTROL = this.CONTROL;
            other.ECHANGE_MAJ_FILE_ID = this.ECHANGE_MAJ_FILE_ID;
            other.ECHANGE_MAJ_ID = this.ECHANGE_MAJ_ID;
            other.SOURCE_COTISATION_ID = this.SOURCE_COTISATION_ID;

        }

        public void copyKeysDataTo(insert_tgr_janv_23Struct other) {

            other.ID = this.ID;

        }


        private String readString(ObjectInputStream dis) throws IOException {
            String strReturn = null;
            int length = 0;
            length = dis.readInt();
            if (length == -1) {
                strReturn = null;
            } else {
                if (length > commonByteArray_FM6_TGR_INSERT_JAN23.length) {
                    if (length < 1024 && commonByteArray_FM6_TGR_INSERT_JAN23.length == 0) {
                        commonByteArray_FM6_TGR_INSERT_JAN23 = new byte[1024];
                    } else {
                        commonByteArray_FM6_TGR_INSERT_JAN23 = new byte[2 * length];
                    }
                }
                dis.readFully(commonByteArray_FM6_TGR_INSERT_JAN23, 0, length);
                strReturn = new String(commonByteArray_FM6_TGR_INSERT_JAN23, 0, length, utf8Charset);
            }
            return strReturn;
        }

        private void writeString(String str, ObjectOutputStream dos) throws IOException {
            if (str == null) {
                dos.writeInt(-1);
            } else {
                byte[] byteArray = str.getBytes(utf8Charset);
                dos.writeInt(byteArray.length);
                dos.write(byteArray);
            }
        }

        public void readData(ObjectInputStream dis) {

            synchronized (commonByteArrayLock_FM6_TGR_INSERT_JAN23) {

                try {

                    int length = 0;

                    this.ID = dis.readLong();

                    this.NUM_PPR = readString(dis);

                    length = dis.readByte();
                    if (length == -1) {
                        this.MONTANT_COTISATION = null;
                    } else {
                        this.MONTANT_COTISATION = dis.readFloat();
                    }

                    this.TYPE_COTISATION = readString(dis);

                    this.COTISATION_CALCULER = readString(dis);

                    this.CONTROL = dis.readByte();

                    length = dis.readByte();
                    if (length == -1) {
                        this.ECHANGE_MAJ_FILE_ID = null;
                    } else {
                        this.ECHANGE_MAJ_FILE_ID = dis.readLong();
                    }

                    this.ECHANGE_MAJ_ID = readString(dis);

                    this.SOURCE_COTISATION_ID = readString(dis);

                } catch (IOException e) {
                    throw new RuntimeException(e);


                }


            }


        }

        public void writeData(ObjectOutputStream dos) {
            try {


                // long

                dos.writeLong(this.ID);

                // String

                writeString(this.NUM_PPR, dos);

                // Float

                if (this.MONTANT_COTISATION == null) {
                    dos.writeByte(-1);
                } else {
                    dos.writeByte(0);
                    dos.writeFloat(this.MONTANT_COTISATION);
                }

                // String

                writeString(this.TYPE_COTISATION, dos);

                // String

                writeString(this.COTISATION_CALCULER, dos);

                // byte

                dos.writeByte(this.CONTROL);

                // Long

                if (this.ECHANGE_MAJ_FILE_ID == null) {
                    dos.writeByte(-1);
                } else {
                    dos.writeByte(0);
                    dos.writeLong(this.ECHANGE_MAJ_FILE_ID);
                }

                // String

                writeString(this.ECHANGE_MAJ_ID, dos);

                // String

                writeString(this.SOURCE_COTISATION_ID, dos);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }


        }


        public String toString() {

            StringBuilder sb = new StringBuilder();
            sb.append(super.toString());
            sb.append("[");
            sb.append("ID=" + String.valueOf(ID));
            sb.append(",NUM_PPR=" + NUM_PPR);
            sb.append(",MONTANT_COTISATION=" + String.valueOf(MONTANT_COTISATION));
            sb.append(",TYPE_COTISATION=" + TYPE_COTISATION);
            sb.append(",COTISATION_CALCULER=" + COTISATION_CALCULER);
            sb.append(",CONTROL=" + String.valueOf(CONTROL));
            sb.append(",ECHANGE_MAJ_FILE_ID=" + String.valueOf(ECHANGE_MAJ_FILE_ID));
            sb.append(",ECHANGE_MAJ_ID=" + ECHANGE_MAJ_ID);
            sb.append(",SOURCE_COTISATION_ID=" + SOURCE_COTISATION_ID);
            sb.append("]");

            return sb.toString();
        }

        /**
         * Compare keys
         */
        public int compareTo(insert_tgr_janv_23Struct other) {

            int returnValue = -1;

            returnValue = checkNullsAndCompare(this.ID, other.ID);
            if (returnValue != 0) {
                return returnValue;
            }


            return returnValue;
        }


        private int checkNullsAndCompare(Object object1, Object object2) {
            int returnValue = 0;
            if (object1 instanceof Comparable && object2 instanceof Comparable) {
                returnValue = ((Comparable) object1).compareTo(object2);
            } else if (object1 != null && object2 != null) {
                returnValue = compareStrings(object1.toString(), object2.toString());
            } else if (object1 == null && object2 != null) {
                returnValue = 1;
            } else if (object1 != null && object2 == null) {
                returnValue = -1;
            } else {
                returnValue = 0;
            }

            return returnValue;
        }

        private int compareStrings(String string1, String string2) {
            return string1.compareTo(string2);
        }


    }

    public static class row1Struct implements IPersistableRow<row1Struct> {
        final static byte[] commonByteArrayLock_FM6_TGR_INSERT_JAN23 = new byte[0];
        static byte[] commonByteArray_FM6_TGR_INSERT_JAN23 = new byte[0];


        public String NUM_PPR;

        public String getNUM_PPR() {
            return this.NUM_PPR;
        }

        public Float MONTANT_COTISATION;

        public Float getMONTANT_COTISATION() {
            return this.MONTANT_COTISATION;
        }

        public String TYPE_COTISATION;

        public String getTYPE_COTISATION() {
            return this.TYPE_COTISATION;
        }

        public Long ECHANGE_MAJ_FILE_ID;

        public Long getECHANGE_MAJ_FILE_ID() {
            return this.ECHANGE_MAJ_FILE_ID;
        }

        public Long ECHANGE_MAJ_ID;

        public Long getECHANGE_MAJ_ID() {
            return this.ECHANGE_MAJ_ID;
        }

        public Long SOURCE_COTISATION_ID;

        public Long getSOURCE_COTISATION_ID() {
            return this.SOURCE_COTISATION_ID;
        }


        private String readString(ObjectInputStream dis) throws IOException {
            String strReturn = null;
            int length = 0;
            length = dis.readInt();
            if (length == -1) {
                strReturn = null;
            } else {
                if (length > commonByteArray_FM6_TGR_INSERT_JAN23.length) {
                    if (length < 1024 && commonByteArray_FM6_TGR_INSERT_JAN23.length == 0) {
                        commonByteArray_FM6_TGR_INSERT_JAN23 = new byte[1024];
                    } else {
                        commonByteArray_FM6_TGR_INSERT_JAN23 = new byte[2 * length];
                    }
                }
                dis.readFully(commonByteArray_FM6_TGR_INSERT_JAN23, 0, length);
                strReturn = new String(commonByteArray_FM6_TGR_INSERT_JAN23, 0, length, utf8Charset);
            }
            return strReturn;
        }

        private void writeString(String str, ObjectOutputStream dos) throws IOException {
            if (str == null) {
                dos.writeInt(-1);
            } else {
                byte[] byteArray = str.getBytes(utf8Charset);
                dos.writeInt(byteArray.length);
                dos.write(byteArray);
            }
        }

        public void readData(ObjectInputStream dis) {

            synchronized (commonByteArrayLock_FM6_TGR_INSERT_JAN23) {

                try {

                    int length = 0;

                    this.NUM_PPR = readString(dis);

                    length = dis.readByte();
                    if (length == -1) {
                        this.MONTANT_COTISATION = null;
                    } else {
                        this.MONTANT_COTISATION = dis.readFloat();
                    }

                    this.TYPE_COTISATION = readString(dis);

                    length = dis.readByte();
                    if (length == -1) {
                        this.ECHANGE_MAJ_FILE_ID = null;
                    } else {
                        this.ECHANGE_MAJ_FILE_ID = dis.readLong();
                    }

                    length = dis.readByte();
                    if (length == -1) {
                        this.ECHANGE_MAJ_ID = null;
                    } else {
                        this.ECHANGE_MAJ_ID = dis.readLong();
                    }

                    length = dis.readByte();
                    if (length == -1) {
                        this.SOURCE_COTISATION_ID = null;
                    } else {
                        this.SOURCE_COTISATION_ID = dis.readLong();
                    }

                } catch (IOException e) {
                    throw new RuntimeException(e);


                }


            }


        }

        public void writeData(ObjectOutputStream dos) {
            try {


                // String

                writeString(this.NUM_PPR, dos);

                // Float

                if (this.MONTANT_COTISATION == null) {
                    dos.writeByte(-1);
                } else {
                    dos.writeByte(0);
                    dos.writeFloat(this.MONTANT_COTISATION);
                }

                // String

                writeString(this.TYPE_COTISATION, dos);

                // Long

                if (this.ECHANGE_MAJ_FILE_ID == null) {
                    dos.writeByte(-1);
                } else {
                    dos.writeByte(0);
                    dos.writeLong(this.ECHANGE_MAJ_FILE_ID);
                }

                // Long

                if (this.ECHANGE_MAJ_ID == null) {
                    dos.writeByte(-1);
                } else {
                    dos.writeByte(0);
                    dos.writeLong(this.ECHANGE_MAJ_ID);
                }

                // Long

                if (this.SOURCE_COTISATION_ID == null) {
                    dos.writeByte(-1);
                } else {
                    dos.writeByte(0);
                    dos.writeLong(this.SOURCE_COTISATION_ID);
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }


        }


        public String toString() {

            StringBuilder sb = new StringBuilder();
            sb.append(super.toString());
            sb.append("[");
            sb.append("NUM_PPR=" + NUM_PPR);
            sb.append(",MONTANT_COTISATION=" + String.valueOf(MONTANT_COTISATION));
            sb.append(",TYPE_COTISATION=" + TYPE_COTISATION);
            sb.append(",ECHANGE_MAJ_FILE_ID=" + String.valueOf(ECHANGE_MAJ_FILE_ID));
            sb.append(",ECHANGE_MAJ_ID=" + String.valueOf(ECHANGE_MAJ_ID));
            sb.append(",SOURCE_COTISATION_ID=" + String.valueOf(SOURCE_COTISATION_ID));
            sb.append("]");

            return sb.toString();
        }

        /**
         * Compare keys
         */
        public int compareTo(row1Struct other) {

            int returnValue = -1;

            return returnValue;
        }


        private int checkNullsAndCompare(Object object1, Object object2) {
            int returnValue = 0;
            if (object1 instanceof Comparable && object2 instanceof Comparable) {
                returnValue = ((Comparable) object1).compareTo(object2);
            } else if (object1 != null && object2 != null) {
                returnValue = compareStrings(object1.toString(), object2.toString());
            } else if (object1 == null && object2 != null) {
                returnValue = 1;
            } else if (object1 != null && object2 == null) {
                returnValue = -1;
            } else {
                returnValue = 0;
            }

            return returnValue;
        }

        private int compareStrings(String string1, String string2) {
            return string1.compareTo(string2);
        }


    }

    public void tFileInputDelimited_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
        globalMap.put("tFileInputDelimited_1_SUBPROCESS_STATE", 0);

        final boolean execStat = this.execStat;

        String iterateId = "";


        String currentComponent = "";
        java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

        try {
            // TDI-39566 avoid throwing an useless Exception
            boolean resumeIt = true;
            if (globalResumeTicket == false && resumeEntryMethodName != null) {
                String currentMethodName = new Exception().getStackTrace()[0].getMethodName();
                resumeIt = resumeEntryMethodName.equals(currentMethodName);
            }
            if (resumeIt || globalResumeTicket) { //start the resume
                globalResumeTicket = true;


                row1Struct row1 = new row1Struct();
                insert_tgr_janv_23Struct insert_tgr_janv_23 = new insert_tgr_janv_23Struct();


                /**
                 * [tDBOutput_1 begin ] start
                 */


                ok_Hash.put("tDBOutput_1", false);
                start_Hash.put("tDBOutput_1", System.currentTimeMillis());


                currentComponent = "tDBOutput_1";


                if (execStat) {
                    runStat.updateStatOnConnection(resourceMap, iterateId, 0, 0, "insert_tgr_janv_23");
                }

                int tos_count_tDBOutput_1 = 0;


                int nb_line_tDBOutput_1 = 0;
                int nb_line_update_tDBOutput_1 = 0;
                int nb_line_inserted_tDBOutput_1 = 0;
                int nb_line_deleted_tDBOutput_1 = 0;
                int nb_line_rejected_tDBOutput_1 = 0;

                int deletedCount_tDBOutput_1 = 0;
                int updatedCount_tDBOutput_1 = 0;
                int insertedCount_tDBOutput_1 = 0;
                int rejectedCount_tDBOutput_1 = 0;
                String dbschema_tDBOutput_1 = null;
                String tableName_tDBOutput_1 = null;
                boolean whetherReject_tDBOutput_1 = false;

                java.util.Calendar calendar_tDBOutput_1 = java.util.Calendar.getInstance();
                long year1_tDBOutput_1 = TalendDate.parseDate("yyyy-MM-dd", "0001-01-01").getTime();
                long year2_tDBOutput_1 = TalendDate.parseDate("yyyy-MM-dd", "1753-01-01").getTime();
                long year10000_tDBOutput_1 = TalendDate.parseDate("yyyy-MM-dd HH:mm:ss", "9999-12-31 24:00:00").getTime();
                long date_tDBOutput_1;

                java.util.Calendar calendar_datetimeoffset_tDBOutput_1 = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));


                java.sql.Connection conn_tDBOutput_1 = null;
                String dbUser_tDBOutput_1 = null;
                dbschema_tDBOutput_1 = "";
                String driverClass_tDBOutput_1 = "net.sourceforge.jtds.jdbc.Driver";

                Class.forName(driverClass_tDBOutput_1);
                String port_tDBOutput_1 = port;
                String dbname_tDBOutput_1 = name;
                String url_tDBOutput_1 = url;
                if (!"".equals(port_tDBOutput_1)) {
                    url_tDBOutput_1 += ":" + port;
                }
                if (!"".equals(dbname_tDBOutput_1)) {
                    url_tDBOutput_1 += "//" + name;
                }
                url_tDBOutput_1 += ";appName=" + projectName + ";" + "";
                dbUser_tDBOutput_1 = username;
                final String decryptedPassword_tDBOutput_1 = password;//PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:1tr4BHYYJheY5xkh1Cu2DcGG/6c2WKbiT8yYL/BQyXKBjRDSVJoicQ==");

                String dbPwd_tDBOutput_1 = decryptedPassword_tDBOutput_1;
                conn_tDBOutput_1 = java.sql.DriverManager.getConnection(url_tDBOutput_1, dbUser_tDBOutput_1, dbPwd_tDBOutput_1);

                resourceMap.put("conn_tDBOutput_1", conn_tDBOutput_1);

                conn_tDBOutput_1.setAutoCommit(false);
                int commitEvery_tDBOutput_1 = 10000;
                int commitCounter_tDBOutput_1 = 0;

                int batchSize_tDBOutput_1 = 10000;
                int batchSizeCounter_tDBOutput_1 = 0;

                if (dbschema_tDBOutput_1 == null || dbschema_tDBOutput_1.trim().length() == 0) {
                    tableName_tDBOutput_1 = "EXCHANGE_CONTROLLER";
                } else {
                    tableName_tDBOutput_1 = dbschema_tDBOutput_1 + "].[" + "EXCHANGE_CONTROLLER";
                }
                int count_tDBOutput_1 = 0;

                boolean whetherExist_tDBOutput_1 = false;
                try (java.sql.Statement isExistStmt_tDBOutput_1 = conn_tDBOutput_1.createStatement()) {
                    try {
                        isExistStmt_tDBOutput_1.execute("SELECT TOP 1 1 FROM [" + tableName_tDBOutput_1 + "]");
                        whetherExist_tDBOutput_1 = true;
                    } catch (Exception e) {
                        whetherExist_tDBOutput_1 = false;
                    }
                }
                if (whetherExist_tDBOutput_1) {
                    try (java.sql.Statement stmtDrop_tDBOutput_1 = conn_tDBOutput_1.createStatement()) {
                        stmtDrop_tDBOutput_1.execute("DROP TABLE [" + tableName_tDBOutput_1 + "]");
                    }
                }
                try (java.sql.Statement stmtCreate_tDBOutput_1 = conn_tDBOutput_1.createStatement()) {
                    stmtCreate_tDBOutput_1.execute("CREATE TABLE [" + tableName_tDBOutput_1 + "]([ID] BIGINT  not null ,[NUM_PPR] VARCHAR(255)  ,[MONTANT_COTISATION] REAL ,[TYPE_COTISATION] VARCHAR(255)  ,[COTISATION_CALCULER] VARCHAR(255)  ,[CONTROL] BIT  not null ,[ECHANGE_MAJ_FILE_ID] BIGINT ,[ECHANGE_MAJ_ID] VARCHAR(19)  ,[SOURCE_COTISATION_ID] VARCHAR(19)  ,primary key([ID]))");
                }
                String insert_tDBOutput_1 = "INSERT INTO [" + tableName_tDBOutput_1 + "] ([ID],[NUM_PPR],[MONTANT_COTISATION],[TYPE_COTISATION],[COTISATION_CALCULER],[CONTROL],[ECHANGE_MAJ_FILE_ID],[ECHANGE_MAJ_ID],[SOURCE_COTISATION_ID]) VALUES (?,?,?,?,?,?,?,?,?)";
                java.sql.PreparedStatement pstmt_tDBOutput_1 = conn_tDBOutput_1.prepareStatement(insert_tDBOutput_1);
                resourceMap.put("pstmt_tDBOutput_1", pstmt_tDBOutput_1);


/**
 * [tDBOutput_1 begin ] stop
 */


                /**
                 * [tMap_1 begin ] start
                 */


                ok_Hash.put("tMap_1", false);
                start_Hash.put("tMap_1", System.currentTimeMillis());


                currentComponent = "tMap_1";


                if (execStat) {
                    runStat.updateStatOnConnection(resourceMap, iterateId, 0, 0, "row1");
                }

                int tos_count_tMap_1 = 0;


// ###############################
// # Lookup's keys initialization
// ###############################        

// ###############################
// # Vars initialization
                class Var__tMap_1__Struct {
                }
                Var__tMap_1__Struct Var__tMap_1 = new Var__tMap_1__Struct();
// ###############################

// ###############################
// # Outputs initialization
                insert_tgr_janv_23Struct insert_tgr_janv_23_tmp = new insert_tgr_janv_23Struct();
// ###############################


/**
 * [tMap_1 begin ] stop
 */


                /**
                 * [tFileInputDelimited_1 begin ] start
                 */


                ok_Hash.put("tFileInputDelimited_1", false);
                start_Hash.put("tFileInputDelimited_1", System.currentTimeMillis());


                currentComponent = "tFileInputDelimited_1";


                int tos_count_tFileInputDelimited_1 = 0;


                final RowState rowstate_tFileInputDelimited_1 = new RowState();


                int nb_line_tFileInputDelimited_1 = 0;
                int footer_tFileInputDelimited_1 = 0;
                int totalLinetFileInputDelimited_1 = 0;
                int limittFileInputDelimited_1 = -1;
                int lastLinetFileInputDelimited_1 = -1;

                char fieldSeparator_tFileInputDelimited_1[] = null;

                //support passing value (property: Field Separator) by 'context.fs' or 'globalMap.get("fs")'.
                if (((String) ",").length() > 0) {
                    fieldSeparator_tFileInputDelimited_1 = ((String) ",").toCharArray();
                } else {
                    throw new IllegalArgumentException("Field Separator must be assigned a char.");
                }

                char rowSeparator_tFileInputDelimited_1[] = null;

                //support passing value (property: Row Separator) by 'context.rs' or 'globalMap.get("rs")'.
                if (((String) "\n").length() > 0) {
                    rowSeparator_tFileInputDelimited_1 = ((String) "\n").toCharArray();
                } else {
                    throw new IllegalArgumentException("Row Separator must be assigned a char.");
                }

                Object filename_tFileInputDelimited_1 = fileUrl;
                com.talend.csv.CSVReader csvReadertFileInputDelimited_1 = null;

                try {
                    String[] rowtFileInputDelimited_1 = null;
                    int currentLinetFileInputDelimited_1 = 0;
                    int outputLinetFileInputDelimited_1 = 0;
                    try {//TD110 begin
                        if (filename_tFileInputDelimited_1 instanceof java.io.InputStream) {

                            int footer_value_tFileInputDelimited_1 = 0;
                            if (footer_value_tFileInputDelimited_1 > 0) {
                                throw new Exception("When the input source is a stream,footer shouldn't be bigger than 0.");
                            }

                            csvReadertFileInputDelimited_1 = new com.talend.csv.CSVReader((java.io.InputStream) filename_tFileInputDelimited_1, fieldSeparator_tFileInputDelimited_1[0], "US-ASCII");
                        } else {
                            csvReadertFileInputDelimited_1 = new com.talend.csv.CSVReader(new java.io.BufferedReader(new java.io.InputStreamReader(
                                    new java.io.FileInputStream(String.valueOf(filename_tFileInputDelimited_1)), "US-ASCII")), fieldSeparator_tFileInputDelimited_1[0]);
                        }


                        csvReadertFileInputDelimited_1.setTrimWhitespace(false);
                        if ((rowSeparator_tFileInputDelimited_1[0] != '\n') && (rowSeparator_tFileInputDelimited_1[0] != '\r'))
                            csvReadertFileInputDelimited_1.setLineEnd("" + rowSeparator_tFileInputDelimited_1[0]);

                        csvReadertFileInputDelimited_1.setQuoteChar('"');

                        csvReadertFileInputDelimited_1.setEscapeChar(csvReadertFileInputDelimited_1.getQuoteChar());


                        if (footer_tFileInputDelimited_1 > 0) {
                            for (totalLinetFileInputDelimited_1 = 0; totalLinetFileInputDelimited_1 < 1; totalLinetFileInputDelimited_1++) {
                                csvReadertFileInputDelimited_1.readNext();
                            }
                            csvReadertFileInputDelimited_1.setSkipEmptyRecords(false);
                            while (csvReadertFileInputDelimited_1.readNext()) {


                                totalLinetFileInputDelimited_1++;


                            }
                            int lastLineTemptFileInputDelimited_1 = totalLinetFileInputDelimited_1 - footer_tFileInputDelimited_1 < 0 ? 0 : totalLinetFileInputDelimited_1 - footer_tFileInputDelimited_1;
                            if (lastLinetFileInputDelimited_1 > 0) {
                                lastLinetFileInputDelimited_1 = lastLinetFileInputDelimited_1 < lastLineTemptFileInputDelimited_1 ? lastLinetFileInputDelimited_1 : lastLineTemptFileInputDelimited_1;
                            } else {
                                lastLinetFileInputDelimited_1 = lastLineTemptFileInputDelimited_1;
                            }

                            csvReadertFileInputDelimited_1.close();
                            if (filename_tFileInputDelimited_1 instanceof java.io.InputStream) {
                                csvReadertFileInputDelimited_1 = new com.talend.csv.CSVReader((java.io.InputStream) filename_tFileInputDelimited_1, fieldSeparator_tFileInputDelimited_1[0], "US-ASCII");
                            } else {
                                csvReadertFileInputDelimited_1 = new com.talend.csv.CSVReader(new java.io.BufferedReader(new java.io.InputStreamReader(
                                        new java.io.FileInputStream(String.valueOf(filename_tFileInputDelimited_1)), "US-ASCII")), fieldSeparator_tFileInputDelimited_1[0]);
                            }
                            csvReadertFileInputDelimited_1.setTrimWhitespace(false);
                            if ((rowSeparator_tFileInputDelimited_1[0] != '\n') && (rowSeparator_tFileInputDelimited_1[0] != '\r'))
                                csvReadertFileInputDelimited_1.setLineEnd("" + rowSeparator_tFileInputDelimited_1[0]);

                            csvReadertFileInputDelimited_1.setQuoteChar('"');

                            csvReadertFileInputDelimited_1.setEscapeChar(csvReadertFileInputDelimited_1.getQuoteChar());

                        }

                        if (limittFileInputDelimited_1 != 0) {
                            for (currentLinetFileInputDelimited_1 = 0; currentLinetFileInputDelimited_1 < 1; currentLinetFileInputDelimited_1++) {
                                csvReadertFileInputDelimited_1.readNext();
                            }
                        }
                        csvReadertFileInputDelimited_1.setSkipEmptyRecords(false);

                    } catch (Exception e) {


                        System.err.println(e.getMessage());

                    }//TD110 end


                    while (limittFileInputDelimited_1 != 0 && csvReadertFileInputDelimited_1 != null && csvReadertFileInputDelimited_1.readNext()) {
                        rowstate_tFileInputDelimited_1.reset();

                        rowtFileInputDelimited_1 = csvReadertFileInputDelimited_1.getValues();


                        currentLinetFileInputDelimited_1++;

                        if (lastLinetFileInputDelimited_1 > -1 && currentLinetFileInputDelimited_1 > lastLinetFileInputDelimited_1) {
                            break;
                        }
                        outputLinetFileInputDelimited_1++;
                        if (limittFileInputDelimited_1 > 0 && outputLinetFileInputDelimited_1 > limittFileInputDelimited_1) {
                            break;
                        }


                        row1 = null;

                        boolean whetherReject_tFileInputDelimited_1 = false;
                        row1 = new row1Struct();
                        try {

                            char fieldSeparator_tFileInputDelimited_1_ListType[] = null;
                            //support passing value (property: Field Separator) by 'context.fs' or 'globalMap.get("fs")'.
                            if (((String) ",").length() > 0) {
                                fieldSeparator_tFileInputDelimited_1_ListType = ((String) ",").toCharArray();
                            } else {
                                throw new IllegalArgumentException("Field Separator must be assigned a char.");
                            }
                            if (rowtFileInputDelimited_1.length == 1 && ("\015").equals(rowtFileInputDelimited_1[0])) {//empty line when row separator is '\n'

                                row1.NUM_PPR = null;

                                row1.MONTANT_COTISATION = null;

                                row1.TYPE_COTISATION = null;

                                row1.ECHANGE_MAJ_FILE_ID = null;

                                row1.ECHANGE_MAJ_ID = null;

                                row1.SOURCE_COTISATION_ID = null;

                            } else {

                                int columnIndexWithD_tFileInputDelimited_1 = 0; //Column Index

                                columnIndexWithD_tFileInputDelimited_1 = 0;


                                if (columnIndexWithD_tFileInputDelimited_1 < rowtFileInputDelimited_1.length) {


                                    row1.NUM_PPR = rowtFileInputDelimited_1[columnIndexWithD_tFileInputDelimited_1];


                                } else {


                                    row1.NUM_PPR = null;


                                }


                                columnIndexWithD_tFileInputDelimited_1 = 1;


                                if (columnIndexWithD_tFileInputDelimited_1 < rowtFileInputDelimited_1.length) {


                                    if (rowtFileInputDelimited_1[columnIndexWithD_tFileInputDelimited_1].length() > 0) {
                                        try {

                                            row1.MONTANT_COTISATION = ParserUtils.parseTo_Float(rowtFileInputDelimited_1[columnIndexWithD_tFileInputDelimited_1]);


                                        } catch (Exception ex_tFileInputDelimited_1) {
                                            rowstate_tFileInputDelimited_1.setException(new RuntimeException(String.format("Couldn't parse value for column '%s' in '%s', value is '%s'. Details: %s",
                                                    "MONTANT_COTISATION", "row1", rowtFileInputDelimited_1[columnIndexWithD_tFileInputDelimited_1], ex_tFileInputDelimited_1), ex_tFileInputDelimited_1));
                                        }
                                    } else {


                                        row1.MONTANT_COTISATION = null;


                                    }


                                } else {


                                    row1.MONTANT_COTISATION = null;


                                }


                                columnIndexWithD_tFileInputDelimited_1 = 2;


                                if (columnIndexWithD_tFileInputDelimited_1 < rowtFileInputDelimited_1.length) {


                                    row1.TYPE_COTISATION = rowtFileInputDelimited_1[columnIndexWithD_tFileInputDelimited_1];


                                } else {


                                    row1.TYPE_COTISATION = null;


                                }


                                columnIndexWithD_tFileInputDelimited_1 = 3;


                                if (columnIndexWithD_tFileInputDelimited_1 < rowtFileInputDelimited_1.length) {


                                    if (rowtFileInputDelimited_1[columnIndexWithD_tFileInputDelimited_1].length() > 0) {
                                        try {

                                            row1.ECHANGE_MAJ_FILE_ID = ParserUtils.parseTo_Long(rowtFileInputDelimited_1[columnIndexWithD_tFileInputDelimited_1]);


                                        } catch (Exception ex_tFileInputDelimited_1) {
                                            rowstate_tFileInputDelimited_1.setException(new RuntimeException(String.format("Couldn't parse value for column '%s' in '%s', value is '%s'. Details: %s",
                                                    "ECHANGE_MAJ_FILE_ID", "row1", rowtFileInputDelimited_1[columnIndexWithD_tFileInputDelimited_1], ex_tFileInputDelimited_1), ex_tFileInputDelimited_1));
                                        }
                                    } else {


                                        row1.ECHANGE_MAJ_FILE_ID = null;


                                    }


                                } else {


                                    row1.ECHANGE_MAJ_FILE_ID = null;


                                }


                                columnIndexWithD_tFileInputDelimited_1 = 4;


                                if (columnIndexWithD_tFileInputDelimited_1 < rowtFileInputDelimited_1.length) {


                                    if (rowtFileInputDelimited_1[columnIndexWithD_tFileInputDelimited_1].length() > 0) {
                                        try {

                                            row1.ECHANGE_MAJ_ID = ParserUtils.parseTo_Long(rowtFileInputDelimited_1[columnIndexWithD_tFileInputDelimited_1]);


                                        } catch (Exception ex_tFileInputDelimited_1) {
                                            rowstate_tFileInputDelimited_1.setException(new RuntimeException(String.format("Couldn't parse value for column '%s' in '%s', value is '%s'. Details: %s",
                                                    "ECHANGE_MAJ_ID", "row1", rowtFileInputDelimited_1[columnIndexWithD_tFileInputDelimited_1], ex_tFileInputDelimited_1), ex_tFileInputDelimited_1));
                                        }
                                    } else {


                                        row1.ECHANGE_MAJ_ID = null;


                                    }


                                } else {


                                    row1.ECHANGE_MAJ_ID = null;


                                }


                                columnIndexWithD_tFileInputDelimited_1 = 5;


                                if (columnIndexWithD_tFileInputDelimited_1 < rowtFileInputDelimited_1.length) {


                                    if (rowtFileInputDelimited_1[columnIndexWithD_tFileInputDelimited_1].length() > 0) {
                                        try {

                                            row1.SOURCE_COTISATION_ID = ParserUtils.parseTo_Long(rowtFileInputDelimited_1[columnIndexWithD_tFileInputDelimited_1]);


                                        } catch (Exception ex_tFileInputDelimited_1) {
                                            rowstate_tFileInputDelimited_1.setException(new RuntimeException(String.format("Couldn't parse value for column '%s' in '%s', value is '%s'. Details: %s",
                                                    "SOURCE_COTISATION_ID", "row1", rowtFileInputDelimited_1[columnIndexWithD_tFileInputDelimited_1], ex_tFileInputDelimited_1), ex_tFileInputDelimited_1));
                                        }
                                    } else {


                                        row1.SOURCE_COTISATION_ID = null;


                                    }


                                } else {


                                    row1.SOURCE_COTISATION_ID = null;


                                }


                            }


                            if (rowstate_tFileInputDelimited_1.getException() != null) {
                                throw rowstate_tFileInputDelimited_1.getException();
                            }


                        } catch (Exception e) {
                            whetherReject_tFileInputDelimited_1 = true;

                            System.err.println(e.getMessage());
                            row1 = null;

                        }


/**
 * [tFileInputDelimited_1 begin ] stop
 */

                        /**
                         * [tFileInputDelimited_1 main ] start
                         */


                        currentComponent = "tFileInputDelimited_1";


                        tos_count_tFileInputDelimited_1++;

/**
 * [tFileInputDelimited_1 main ] stop
 */

                        /**
                         * [tFileInputDelimited_1 process_data_begin ] start
                         */


                        currentComponent = "tFileInputDelimited_1";


/**
 * [tFileInputDelimited_1 process_data_begin ] stop
 */
// Start of branch "row1"
                        if (row1 != null) {


                            /**
                             * [tMap_1 main ] start
                             */


                            currentComponent = "tMap_1";


                            if (execStat) {
                                runStat.updateStatOnConnection(iterateId, 1, 1, "row1");
                            }


                            boolean hasCasePrimitiveKeyWithNull_tMap_1 = false;

                            // ###############################
                            // # Input tables (lookups)
                            boolean rejectedInnerJoin_tMap_1 = false;
                            boolean mainRowRejected_tMap_1 = false;

                            // ###############################
                            { // start of Var scope

                                // ###############################
                                // # Vars tables

                                Var__tMap_1__Struct Var = Var__tMap_1;// ###############################
                                // ###############################
                                // # Output tables

                                insert_tgr_janv_23 = null;


// # Output table : 'insert_tgr_janv_23'
                                insert_tgr_janv_23_tmp.ID = Numeric.sequence("seq", 1, 1);
                                insert_tgr_janv_23_tmp.NUM_PPR = row1.NUM_PPR;
                                insert_tgr_janv_23_tmp.MONTANT_COTISATION = row1.MONTANT_COTISATION;
                                insert_tgr_janv_23_tmp.TYPE_COTISATION = row1.TYPE_COTISATION;
                                insert_tgr_janv_23_tmp.COTISATION_CALCULER = (((row1.MONTANT_COTISATION) == null) ? null : (TypeConvert.Float2String(row1.MONTANT_COTISATION)));
                                insert_tgr_janv_23_tmp.CONTROL = 0;
                                insert_tgr_janv_23_tmp.ECHANGE_MAJ_FILE_ID = row1.ECHANGE_MAJ_FILE_ID;
                                insert_tgr_janv_23_tmp.ECHANGE_MAJ_ID = (((row1.ECHANGE_MAJ_ID) == null) ? null : (TypeConvert.Long2String(row1.ECHANGE_MAJ_ID)));
                                insert_tgr_janv_23_tmp.SOURCE_COTISATION_ID = (((row1.SOURCE_COTISATION_ID) == null) ? null : (TypeConvert.Long2String(row1.SOURCE_COTISATION_ID)));
                                insert_tgr_janv_23 = insert_tgr_janv_23_tmp;
// ###############################

                            } // end of Var scope

                            rejectedInnerJoin_tMap_1 = false;


                            tos_count_tMap_1++;

/**
 * [tMap_1 main ] stop
 */

                            /**
                             * [tMap_1 process_data_begin ] start
                             */


                            currentComponent = "tMap_1";


/**
 * [tMap_1 process_data_begin ] stop
 */
// Start of branch "insert_tgr_janv_23"
                            if (insert_tgr_janv_23 != null) {


                                /**
                                 * [tDBOutput_1 main ] start
                                 */


                                currentComponent = "tDBOutput_1";


                                if (execStat) {
                                    runStat.updateStatOnConnection(iterateId, 1, 1, "insert_tgr_janv_23");
                                }


                                whetherReject_tDBOutput_1 = false;
                                pstmt_tDBOutput_1.setLong(1, insert_tgr_janv_23.ID);

                                if (insert_tgr_janv_23.NUM_PPR == null) {
                                    pstmt_tDBOutput_1.setNull(2, java.sql.Types.VARCHAR);
                                } else {
                                    pstmt_tDBOutput_1.setString(2, insert_tgr_janv_23.NUM_PPR);
                                }

                                if (insert_tgr_janv_23.MONTANT_COTISATION == null) {
                                    pstmt_tDBOutput_1.setNull(3, java.sql.Types.FLOAT);
                                } else {
                                    pstmt_tDBOutput_1.setFloat(3, insert_tgr_janv_23.MONTANT_COTISATION);
                                }

                                if (insert_tgr_janv_23.TYPE_COTISATION == null) {
                                    pstmt_tDBOutput_1.setNull(4, java.sql.Types.VARCHAR);
                                } else {
                                    pstmt_tDBOutput_1.setString(4, insert_tgr_janv_23.TYPE_COTISATION);
                                }

                                if (insert_tgr_janv_23.COTISATION_CALCULER == null) {
                                    pstmt_tDBOutput_1.setNull(5, java.sql.Types.VARCHAR);
                                } else {
                                    pstmt_tDBOutput_1.setString(5, insert_tgr_janv_23.COTISATION_CALCULER);
                                }

                                pstmt_tDBOutput_1.setByte(6, insert_tgr_janv_23.CONTROL);

                                if (insert_tgr_janv_23.ECHANGE_MAJ_FILE_ID == null) {
                                    pstmt_tDBOutput_1.setNull(7, java.sql.Types.INTEGER);
                                } else {
                                    pstmt_tDBOutput_1.setLong(7, insert_tgr_janv_23.ECHANGE_MAJ_FILE_ID);
                                }

                                if (insert_tgr_janv_23.ECHANGE_MAJ_ID == null) {
                                    pstmt_tDBOutput_1.setNull(8, java.sql.Types.VARCHAR);
                                } else {
                                    pstmt_tDBOutput_1.setString(8, insert_tgr_janv_23.ECHANGE_MAJ_ID);
                                }

                                if (insert_tgr_janv_23.SOURCE_COTISATION_ID == null) {
                                    pstmt_tDBOutput_1.setNull(9, java.sql.Types.VARCHAR);
                                } else {
                                    pstmt_tDBOutput_1.setString(9, insert_tgr_janv_23.SOURCE_COTISATION_ID);
                                }


                                pstmt_tDBOutput_1.addBatch();
                                nb_line_tDBOutput_1++;


                                batchSizeCounter_tDBOutput_1++;

                                //////////batch execute by batch size///////
                                class LimitBytesHelper_tDBOutput_1 {
                                    public int limitBytePart1(int counter, java.sql.PreparedStatement pstmt_tDBOutput_1) throws Exception {
                                        try {

                                            for (int countEach_tDBOutput_1 : pstmt_tDBOutput_1.executeBatch()) {
                                                if (countEach_tDBOutput_1 == -2 || countEach_tDBOutput_1 == -3) {
                                                    break;
                                                }
                                                counter += countEach_tDBOutput_1;
                                            }

                                        } catch (java.sql.BatchUpdateException e) {

                                            int countSum_tDBOutput_1 = 0;
                                            for (int countEach_tDBOutput_1 : e.getUpdateCounts()) {
                                                counter += (countEach_tDBOutput_1 < 0 ? 0 : countEach_tDBOutput_1);
                                            }


                                            System.err.println(e.getMessage());

                                        }
                                        return counter;
                                    }

                                    public int limitBytePart2(int counter, java.sql.PreparedStatement pstmt_tDBOutput_1) throws Exception {
                                        try {

                                            for (int countEach_tDBOutput_1 : pstmt_tDBOutput_1.executeBatch()) {
                                                if (countEach_tDBOutput_1 == -2 || countEach_tDBOutput_1 == -3) {
                                                    break;
                                                }
                                                counter += countEach_tDBOutput_1;
                                            }

                                        } catch (java.sql.BatchUpdateException e) {


                                            for (int countEach_tDBOutput_1 : e.getUpdateCounts()) {
                                                counter += (countEach_tDBOutput_1 < 0 ? 0 : countEach_tDBOutput_1);
                                            }


                                            System.err.println(e.getMessage());

                                        }
                                        return counter;
                                    }
                                }
                                if ((batchSize_tDBOutput_1 > 0) && (batchSize_tDBOutput_1 <= batchSizeCounter_tDBOutput_1)) {


                                    insertedCount_tDBOutput_1 = new LimitBytesHelper_tDBOutput_1().limitBytePart1(insertedCount_tDBOutput_1, pstmt_tDBOutput_1);


                                    batchSizeCounter_tDBOutput_1 = 0;
                                }


                                ////////////commit every////////////

                                commitCounter_tDBOutput_1++;
                                if (commitEvery_tDBOutput_1 <= commitCounter_tDBOutput_1) {
                                    if ((batchSize_tDBOutput_1 > 0) && (batchSizeCounter_tDBOutput_1 > 0)) {

                                        insertedCount_tDBOutput_1 = new LimitBytesHelper_tDBOutput_1().limitBytePart1(insertedCount_tDBOutput_1, pstmt_tDBOutput_1);

                                        batchSizeCounter_tDBOutput_1 = 0;
                                    }

                                    conn_tDBOutput_1.commit();

                                    commitCounter_tDBOutput_1 = 0;
                                }


                                tos_count_tDBOutput_1++;

/**
 * [tDBOutput_1 main ] stop
 */

                                /**
                                 * [tDBOutput_1 process_data_begin ] start
                                 */


                                currentComponent = "tDBOutput_1";


/**
 * [tDBOutput_1 process_data_begin ] stop
 */

                                /**
                                 * [tDBOutput_1 process_data_end ] start
                                 */


                                currentComponent = "tDBOutput_1";


/**
 * [tDBOutput_1 process_data_end ] stop
 */

                            } // End of branch "insert_tgr_janv_23"


                            /**
                             * [tMap_1 process_data_end ] start
                             */


                            currentComponent = "tMap_1";


/**
 * [tMap_1 process_data_end ] stop
 */

                        } // End of branch "row1"


                        /**
                         * [tFileInputDelimited_1 process_data_end ] start
                         */


                        currentComponent = "tFileInputDelimited_1";


/**
 * [tFileInputDelimited_1 process_data_end ] stop
 */

                        /**
                         * [tFileInputDelimited_1 end ] start
                         */


                        currentComponent = "tFileInputDelimited_1";


                        nb_line_tFileInputDelimited_1++;
                    }

                } finally {
                    if (!(filename_tFileInputDelimited_1 instanceof java.io.InputStream)) {
                        if (csvReadertFileInputDelimited_1 != null) {
                            csvReadertFileInputDelimited_1.close();
                        }
                    }
                    if (csvReadertFileInputDelimited_1 != null) {
                        globalMap.put("tFileInputDelimited_1_NB_LINE", nb_line_tFileInputDelimited_1);
                    }

                }


                ok_Hash.put("tFileInputDelimited_1", true);
                end_Hash.put("tFileInputDelimited_1", System.currentTimeMillis());


/**
 * [tFileInputDelimited_1 end ] stop
 */


                /**
                 * [tMap_1 end ] start
                 */


                currentComponent = "tMap_1";


// ###############################
// # Lookup hashes releasing
// ###############################      


                if (execStat) {
                    runStat.updateStat(resourceMap, iterateId, 2, 0, "row1");
                }


                ok_Hash.put("tMap_1", true);
                end_Hash.put("tMap_1", System.currentTimeMillis());


/**
 * [tMap_1 end ] stop
 */


                /**
                 * [tDBOutput_1 end ] start
                 */


                currentComponent = "tDBOutput_1";


                try {
                    int countSum_tDBOutput_1 = 0;
                    if (pstmt_tDBOutput_1 != null && batchSizeCounter_tDBOutput_1 > 0) {

                        for (int countEach_tDBOutput_1 : pstmt_tDBOutput_1.executeBatch()) {
                            if (countEach_tDBOutput_1 == -2 || countEach_tDBOutput_1 == -3) {
                                break;
                            }
                            countSum_tDBOutput_1 += countEach_tDBOutput_1;
                        }

                    }

                    insertedCount_tDBOutput_1 += countSum_tDBOutput_1;

                } catch (java.sql.BatchUpdateException e) {

                    int countSum_tDBOutput_1 = 0;
                    for (int countEach_tDBOutput_1 : e.getUpdateCounts()) {
                        countSum_tDBOutput_1 += (countEach_tDBOutput_1 < 0 ? 0 : countEach_tDBOutput_1);
                    }

                    insertedCount_tDBOutput_1 += countSum_tDBOutput_1;

                    System.err.println(e.getMessage());

                }
                if (pstmt_tDBOutput_1 != null) {

                    pstmt_tDBOutput_1.close();
                    resourceMap.remove("pstmt_tDBOutput_1");

                }
                resourceMap.put("statementClosed_tDBOutput_1", true);
                conn_tDBOutput_1.commit();

                conn_tDBOutput_1.close();
                resourceMap.put("finish_tDBOutput_1", true);

                nb_line_deleted_tDBOutput_1 = nb_line_deleted_tDBOutput_1 + deletedCount_tDBOutput_1;
                nb_line_update_tDBOutput_1 = nb_line_update_tDBOutput_1 + updatedCount_tDBOutput_1;
                nb_line_inserted_tDBOutput_1 = nb_line_inserted_tDBOutput_1 + insertedCount_tDBOutput_1;
                nb_line_rejected_tDBOutput_1 = nb_line_rejected_tDBOutput_1 + rejectedCount_tDBOutput_1;

                globalMap.put("tDBOutput_1_NB_LINE", nb_line_tDBOutput_1);
                globalMap.put("tDBOutput_1_NB_LINE_UPDATED", nb_line_update_tDBOutput_1);
                globalMap.put("tDBOutput_1_NB_LINE_INSERTED", nb_line_inserted_tDBOutput_1);
                globalMap.put("tDBOutput_1_NB_LINE_DELETED", nb_line_deleted_tDBOutput_1);
                globalMap.put("tDBOutput_1_NB_LINE_REJECTED", nb_line_rejected_tDBOutput_1);


                if (execStat) {
                    runStat.updateStat(resourceMap, iterateId, 2, 0, "insert_tgr_janv_23");
                }


                ok_Hash.put("tDBOutput_1", true);
                end_Hash.put("tDBOutput_1", System.currentTimeMillis());


/**
 * [tDBOutput_1 end ] stop
 */


            }//end the resume


        } catch (Exception e) {

            TalendException te = new TalendException(e, currentComponent, globalMap);

            throw te;
        } catch (Error error) {

            runStat.stopThreadStat();

            throw error;
        } finally {

            try {


                /**
                 * [tFileInputDelimited_1 finally ] start
                 */


                currentComponent = "tFileInputDelimited_1";


/**
 * [tFileInputDelimited_1 finally ] stop
 */


                /**
                 * [tMap_1 finally ] start
                 */


                currentComponent = "tMap_1";


/**
 * [tMap_1 finally ] stop
 */


                /**
                 * [tDBOutput_1 finally ] start
                 */


                currentComponent = "tDBOutput_1";


                try {
                    if (resourceMap.get("statementClosed_tDBOutput_1") == null) {
                        java.sql.PreparedStatement pstmtToClose_tDBOutput_1 = null;
                        if ((pstmtToClose_tDBOutput_1 = (java.sql.PreparedStatement) resourceMap.remove("pstmt_tDBOutput_1")) != null) {
                            pstmtToClose_tDBOutput_1.close();
                        }
                    }
                } finally {
                    if (resourceMap.get("finish_tDBOutput_1") == null) {
                        java.sql.Connection ctn_tDBOutput_1 = null;
                        if ((ctn_tDBOutput_1 = (java.sql.Connection) resourceMap.get("conn_tDBOutput_1")) != null) {
                            try {
                                ctn_tDBOutput_1.close();
                            } catch (java.sql.SQLException sqlEx_tDBOutput_1) {
                                String errorMessage_tDBOutput_1 = "failed to close the connection in tDBOutput_1 :" + sqlEx_tDBOutput_1.getMessage();
                                System.err.println(errorMessage_tDBOutput_1);
                            }
                        }
                    }
                }


/**
 * [tDBOutput_1 finally ] stop
 */


            } catch (Exception e) {
                //ignore
            } catch (Error error) {
                //ignore
            }
            resourceMap = null;
        }


        globalMap.put("tFileInputDelimited_1_SUBPROCESS_STATE", 1);
    }

    public String resuming_logs_dir_path = null;
    public String resuming_checkpoint_path = null;
    public String parent_part_launcher = null;
    private String resumeEntryMethodName = null;
    private boolean globalResumeTicket = false;

    public boolean watch = false;
    // portStats is null, it means don't execute the statistics
    public Integer portStats = null;
    public int portTraces = 4334;
    public String clientHost;
    public String defaultClientHost = "localhost";
    public String contextStr = "Default";
    public boolean isDefaultContext = true;
    public String pid = "0";
    public String rootPid = null;
    public String fatherPid = null;
    public String fatherNode = null;
    public long startTime = 0;
    public boolean isChildJob = false;
    public String log4jLevel = "";

    private boolean enableLogStash;

    private boolean execStat = true;

    private ThreadLocal<java.util.Map<String, String>> threadLocal = new ThreadLocal<java.util.Map<String, String>>() {
        protected java.util.Map<String, String> initialValue() {
            java.util.Map<String, String> threadRunResultMap = new java.util.HashMap<String, String>();
            threadRunResultMap.put("errorCode", null);
            threadRunResultMap.put("status", "");
            return threadRunResultMap;
        }

        ;
    };


    private PropertiesWithType context_param = new PropertiesWithType();
    public java.util.Map<String, Object> parentContextMap = new java.util.HashMap<String, Object>();

    public String status = "";


    public static void main(String[] args) {
        final TgrInsertJob TGR_INSERT_JAN23Class = new TgrInsertJob();

        int exitCode = TGR_INSERT_JAN23Class.runJobInTOS(args);

        System.exit(exitCode);
    }


    public String[][] runJob(String[] args) {

        int exitCode = runJobInTOS(args);
        String[][] bufferValue = new String[][]{{Integer.toString(exitCode)}};

        return bufferValue;
    }

    public boolean hastBufferOutputComponent() {
        boolean hastBufferOutput = false;

        return hastBufferOutput;
    }

    public int runJobInTOS(String[] args) {
        // reset status
        status = "";

        String lastStr = "";
        for (String arg : args) {
            if (arg.equalsIgnoreCase("--context_param")) {
                lastStr = arg;
            } else if (lastStr.equals("")) {
                evalParam(arg);
            } else {
                evalParam(lastStr + " " + arg);
                lastStr = "";
            }
        }
        enableLogStash = "true".equalsIgnoreCase(System.getProperty("monitoring"));


        if (clientHost == null) {
            clientHost = defaultClientHost;
        }

        if (pid == null || "0".equals(pid)) {
            pid = TalendString.getAsciiRandomString(6);
        }

        if (rootPid == null) {
            rootPid = pid;
        }
        if (fatherPid == null) {
            fatherPid = pid;
        } else {
            isChildJob = true;
        }

        if (portStats != null) {
            // portStats = -1; //for testing
            if (portStats < 0 || portStats > 65535) {
                // issue:10869, the portStats is invalid, so this client socket can't open
                System.err.println("The statistics socket port " + portStats + " is invalid.");
                execStat = false;
            }
        } else {
            execStat = false;
        }

        try {
            //call job/subjob with an existing context, like: --context=production. if without this parameter, there will use the default context instead.
            java.io.InputStream inContext = TgrInsertJob.class.getClassLoader().getResourceAsStream("fm6/tgr_insert_jan23_0_1/contexts/" + contextStr + ".properties");
            if (inContext == null) {
                inContext = TgrInsertJob.class.getClassLoader().getResourceAsStream("config/contexts/" + contextStr + ".properties");
            }
            if (inContext != null) {
                //defaultProps is in order to keep the original context value
                if (context != null && context.isEmpty()) {
                    defaultProps.load(inContext);
                    context = new ContextProperties(defaultProps);
                }

                inContext.close();
            } else if (!isDefaultContext) {
                //print info and job continue to run, for case: context_param is not empty.
                System.err.println("Could not find the context " + contextStr);
            }

            if (!context_param.isEmpty()) {
                context.putAll(context_param);
                //set types for params from parentJobs
                for (Object key : context_param.keySet()) {
                    String context_key = key.toString();
                    String context_type = context_param.getContextType(context_key);
                    context.setContextType(context_key, context_type);

                }
            }
            class ContextProcessing {
                private void processContext_0() {
                }

                public void processAllContext() {
                    processContext_0();
                }
            }

            new ContextProcessing().processAllContext();
        } catch (IOException ie) {
            System.err.println("Could not load context " + contextStr);
            ie.printStackTrace();
        }

        // get context value from parent directly
        if (parentContextMap != null && !parentContextMap.isEmpty()) {
        }

        //Resume: init the resumeUtil
        resumeEntryMethodName = ResumeUtil.getResumeEntryMethodName(resuming_checkpoint_path);
        resumeUtil = new ResumeUtil(resuming_logs_dir_path, isChildJob, rootPid);
        resumeUtil.initCommonInfo(pid, rootPid, fatherPid, projectName, jobName, contextStr, jobVersion);

        List<String> parametersToEncrypt = new java.util.ArrayList<String>();
        //Resume: jobStart
        resumeUtil.addLog("JOB_STARTED", "JOB:" + jobName, parent_part_launcher, Thread.currentThread().getId() + "", "", "", "", "", resumeUtil.convertToJsonText(context, parametersToEncrypt));

        if (execStat) {
            try {
                runStat.openSocket(!isChildJob);
                runStat.setAllPID(rootPid, fatherPid, pid, jobName);
                runStat.startThreadStat(clientHost, portStats);
                runStat.updateStatOnJob(RunStat.JOBSTART, fatherNode);
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }


        java.util.concurrent.ConcurrentHashMap<Object, Object> concurrentHashMap = new java.util.concurrent.ConcurrentHashMap<Object, Object>();
        globalMap.put("concurrentHashMap", concurrentHashMap);


        long startUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long endUsedMemory = 0;
        long end = 0;

        startTime = System.currentTimeMillis();


        this.globalResumeTicket = true;//to run tPreJob


        this.globalResumeTicket = false;//to run others jobs

        try {
            errorCode = null;
            tFileInputDelimited_1Process(globalMap);
            if (!"failure".equals(status)) {
                status = "end";
            }
        } catch (TalendException e_tFileInputDelimited_1) {
            globalMap.put("tFileInputDelimited_1_SUBPROCESS_STATE", -1);

            e_tFileInputDelimited_1.printStackTrace();

        }

        this.globalResumeTicket = true;//to run tPostJob


        end = System.currentTimeMillis();

        if (watch) {
            System.out.println((end - startTime) + " milliseconds");
        }

        endUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if (false) {
            System.out.println((endUsedMemory - startUsedMemory) + " bytes memory increase when running : TGR_INSERT_JAN23");
        }


        if (execStat) {
            runStat.updateStatOnJob(RunStat.JOBEND, fatherNode);
            runStat.stopThreadStat();
        }
        int returnCode = 0;
        if (errorCode == null) {
            returnCode = status != null && status.equals("failure") ? 1 : 0;
        } else {
            returnCode = errorCode.intValue();
        }
        resumeUtil.addLog("JOB_ENDED", "JOB:" + jobName, parent_part_launcher, Thread.currentThread().getId() + "", "", "" + returnCode, "", "", "");

        return returnCode;

    }

    // only for OSGi env
    public void destroy() {


    }


    private java.util.Map<String, Object> getSharedConnections4REST() {
        java.util.Map<String, Object> connections = new java.util.HashMap<String, Object>();


        return connections;
    }

    private void evalParam(String arg) {
        if (arg.startsWith("--resuming_logs_dir_path")) {
            resuming_logs_dir_path = arg.substring(25);
        } else if (arg.startsWith("--resuming_checkpoint_path")) {
            resuming_checkpoint_path = arg.substring(27);
        } else if (arg.startsWith("--parent_part_launcher")) {
            parent_part_launcher = arg.substring(23);
        } else if (arg.startsWith("--watch")) {
            watch = true;
        } else if (arg.startsWith("--stat_port=")) {
            String portStatsStr = arg.substring(12);
            if (portStatsStr != null && !portStatsStr.equals("null")) {
                portStats = Integer.parseInt(portStatsStr);
            }
        } else if (arg.startsWith("--trace_port=")) {
            portTraces = Integer.parseInt(arg.substring(13));
        } else if (arg.startsWith("--client_host=")) {
            clientHost = arg.substring(14);
        } else if (arg.startsWith("--context=")) {
            contextStr = arg.substring(10);
            isDefaultContext = false;
        } else if (arg.startsWith("--father_pid=")) {
            fatherPid = arg.substring(13);
        } else if (arg.startsWith("--root_pid=")) {
            rootPid = arg.substring(11);
        } else if (arg.startsWith("--father_node=")) {
            fatherNode = arg.substring(14);
        } else if (arg.startsWith("--pid=")) {
            pid = arg.substring(6);
        } else if (arg.startsWith("--context_type")) {
            String keyValue = arg.substring(15);
            int index = -1;
            if (keyValue != null && (index = keyValue.indexOf('=')) > -1) {
                if (fatherPid == null) {
                    context_param.setContextType(keyValue.substring(0, index), replaceEscapeChars(keyValue.substring(index + 1)));
                } else { // the subjob won't escape the especial chars
                    context_param.setContextType(keyValue.substring(0, index), keyValue.substring(index + 1));
                }

            }

        } else if (arg.startsWith("--context_param")) {
            String keyValue = arg.substring(16);
            int index = -1;
            if (keyValue != null && (index = keyValue.indexOf('=')) > -1) {
                if (fatherPid == null) {
                    context_param.put(keyValue.substring(0, index), replaceEscapeChars(keyValue.substring(index + 1)));
                } else { // the subjob won't escape the especial chars
                    context_param.put(keyValue.substring(0, index), keyValue.substring(index + 1));
                }
            }
        } else if (arg.startsWith("--log4jLevel=")) {
            log4jLevel = arg.substring(13);
        } else if (arg.startsWith("--monitoring") && arg.contains("=")) {//for trunjob call
            final int equal = arg.indexOf('=');
            final String key = arg.substring("--".length(), equal);
            System.setProperty(key, arg.substring(equal + 1));
        }
    }

    private static final String NULL_VALUE_EXPRESSION_IN_COMMAND_STRING_FOR_CHILD_JOB_ONLY = "<TALEND_NULL>";

    private final String[][] escapeChars = {
            {"\\\\", "\\"}, {"\\n", "\n"}, {"\\'", "\'"}, {"\\r", "\r"},
            {"\\f", "\f"}, {"\\b", "\b"}, {"\\t", "\t"}
    };

    private String replaceEscapeChars(String keyValue) {

        if (keyValue == null || ("").equals(keyValue.trim())) {
            return keyValue;
        }

        StringBuilder result = new StringBuilder();
        int currIndex = 0;
        while (currIndex < keyValue.length()) {
            int index = -1;
            // judege if the left string includes escape chars
            for (String[] strArray : escapeChars) {
                index = keyValue.indexOf(strArray[0], currIndex);
                if (index >= 0) {

                    result.append(keyValue.substring(currIndex, index + strArray[0].length()).replace(strArray[0], strArray[1]));
                    currIndex = index + strArray[0].length();
                    break;
                }
            }
            // if the left string doesn't include escape chars, append the left into the result
            if (index < 0) {
                result.append(keyValue.substring(currIndex));
                currIndex = currIndex + keyValue.length();
            }
        }

        return result.toString();
    }

    public Integer getErrorCode() {
        return errorCode;
    }


    public String getStatus() {
        return status;
    }

    ResumeUtil resumeUtil = null;
}
/************************************************************************************************
 *     71890 characters generated by Talend Open Studio for Data Integration 
 *     on the 10 juillet 2023 à 00:47:06 WEST
 ************************************************************************************************/