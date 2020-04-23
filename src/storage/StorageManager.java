package storage;

import common.CatalogDB;
import common.Constants;
import common.Utils;
import console.ConsoleWriter;
import datatypes.*;
import datatypes.base.DT;
import datatypes.base.DT_Numeric;
import helpers.UpdateStatementHelper;
import storage.model.DataRecord;
import storage.model.InternalCondition;
import storage.model.Page;
import storage.model.PointerRecord;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StorageManager {

    private String DEFAULT_DATA_PATH = Constants.DEFAULT_DATA_DIRNAME;

    public boolean createDatabaseQuery(String databaseName) {
        try {
            File dirFile = new File(DEFAULT_DATA_PATH + "/" + databaseName);
            if (dirFile.exists()) {
                return false;
            }
            return dirFile.mkdirs();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean dropDatabaseQuery(String databaseName) {
        try {
            File dirFile = new File(DEFAULT_DATA_PATH + "/" + databaseName);
            if (!dirFile.exists()) {
                System.out.println("Database " + databaseName + " doesn't!");
                return false;
            }
            return dirFile.mkdirs();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean IsdatabaseExists(String databaseName) {
        File databaseDir = new File(databaseName);
        return  databaseDir.exists();
    }

    public boolean createTable(String databaseName, String tableName) {
        try {
            File dirFile = new File(databaseName);
            if (!dirFile.exists()) {
                dirFile.mkdir();
            }
            File file = new File(databaseName + "/" + tableName);
            if (file.exists()) {
                return false;
            }
            if (file.createNewFile()) {
                RandomAccessFile randomAccessFile;
                Page<DataRecord> page = Page.createNewEmptyPage(new DataRecord());
                randomAccessFile = new RandomAccessFile(file, "rw");
                randomAccessFile.setLength(Page.PAGE_SIZE);
                boolean isTableCreated = writePageHeader(randomAccessFile, page);
                randomAccessFile.close();
                return isTableCreated;
            }
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

   
    public boolean checkTableExists(String databaseName, String tableName) {
        boolean IsdatabaseExists = this.IsdatabaseExists(databaseName);
        boolean IsfileExists = new File(databaseName + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION).exists();

        return (IsdatabaseExists && IsfileExists);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean writeRecord(String databaseName, String tableName, DataRecord record) {
        RandomAccessFile randomAccessFile = null;
        try {
            File file = new File(databaseName + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                randomAccessFile = new RandomAccessFile(file, "rw");
                Page page = getPage(randomAccessFile, record, 0);
                if (page == null) return false;
                if (!checkSpaceRequirements(page, record)) {
                    int pageCount = (int) (randomAccessFile.length() / Page.PAGE_SIZE);
                    switch (pageCount) {
                        case 1:
                            PointerRecord pointerRecord = splitPage(randomAccessFile, page, record, 1, 2);
                            Page<PointerRecord> pointerRecordPage = Page.createNewEmptyPage(pointerRecord);
                            pointerRecordPage.setPageNumber(0);
                            pointerRecordPage.setPageType(Page.INTERIOR_TABLE_PAGE);
                            pointerRecordPage.setNumberOfCells((byte) 1);
                            pointerRecordPage.setStartingAddress((short) (pointerRecordPage.getStartingAddress() - pointerRecord.getSize()));
                            pointerRecordPage.setRightNodeAddress(2);
                            pointerRecordPage.getRecordAddressList().add((short) (pointerRecordPage.getStartingAddress() + 1));
                            pointerRecord.setPageNumber(pointerRecordPage.getPageNumber());
                            pointerRecord.setOffset((short) (pointerRecordPage.getStartingAddress() + 1));
                            this.writePageHeader(randomAccessFile, pointerRecordPage);
                            this.writeRecord(randomAccessFile, pointerRecord);
                            break;

                        default:
                            if(pageCount > 1) {
                                PointerRecord pointerRecord1 = splitPage(randomAccessFile, readPageHeader(randomAccessFile, 0), record);
                                if(pointerRecord1 != null && pointerRecord1.getLeftPageNumber() != -1)  {
                                    Page<PointerRecord> rootPage = Page.createNewEmptyPage(pointerRecord1);
                                    rootPage.setPageNumber(0);
                                    rootPage.setPageType(Page.INTERIOR_TABLE_PAGE);
                                    rootPage.setNumberOfCells((byte) 1);
                                    rootPage.setStartingAddress((short)(rootPage.getStartingAddress() - pointerRecord1.getSize()));
                                    rootPage.setRightNodeAddress(pointerRecord1.getPageNumber());
                                    rootPage.getRecordAddressList().add((short) (rootPage.getStartingAddress() + 1));
                                    pointerRecord1.setOffset((short) (rootPage.getStartingAddress() + 1));
                                    this.writePageHeader(randomAccessFile, rootPage);
                                    this.writeRecord(randomAccessFile, pointerRecord1);
                                }
                            }
                            break;
                    }
                    UpdateStatementHelper.incrementRowCount(tableName);
                    randomAccessFile.close();
                    return true;
                }
                short address = (short) getAddress(file, record.getRowId(), page.getPageNumber());
                page.setNumberOfCells((byte)(page.getNumberOfCells() + 1));
                page.setStartingAddress((short) (page.getStartingAddress() - record.getSize() - record.getHeaderSize()));
                if(address == page.getRecordAddressList().size())
                    page.getRecordAddressList().add((short)(page.getStartingAddress() + 1));
                else
                    page.getRecordAddressList().add(address, (short)(page.getStartingAddress() + 1));
                record.setPageLocated(page.getPageNumber());
                record.setOffset((short) (page.getStartingAddress() + 1));
                this.writePageHeader(randomAccessFile, page);
                this.writeRecord(randomAccessFile, record);
                randomAccessFile.close();
            } else {
                ConsoleWriter.displayMessage("File " + tableName + " does not exist");
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @SuppressWarnings("rawtypes")
	private boolean checkSpaceRequirements(Page page, DataRecord record) {
        if (page != null && record != null) {
            short endingAddress = page.getStartingAddress();
            short startingAddress = (short) (Page.getHeaderFixedLength() + (page.getRecordAddressList().size() * Short.BYTES));
            return (record.getSize() + record.getHeaderSize() + Short.BYTES) <= (endingAddress - startingAddress);
        }
        return false;
    }

    @SuppressWarnings("rawtypes")
	private boolean checkSpaceRequirements(Page page, PointerRecord record) {
        if(page != null && record != null) {
            short endingAddress = page.getStartingAddress();
            short startingAddress = (short) (Page.getHeaderFixedLength() + (page.getRecordAddressList().size() * Short.BYTES));
            return (record.getSize() + Short.BYTES) <= (endingAddress - startingAddress);
        }
        return false;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	private PointerRecord splitPage(RandomAccessFile randomAccessFile, Page page, DataRecord record, int pageNumber1, int pageNumber2) {
        try {
            if (page != null && record != null) {
                int location;
                PointerRecord pointerRecord = new PointerRecord();
                if (page.getPageType() == Page.INTERIOR_TABLE_PAGE) {
                    return null;
                }
                location = binarySearch(randomAccessFile, record.getRowId(), page.getNumberOfCells(), ((page.getPageNumber() * Page.PAGE_SIZE) + Page.getHeaderFixedLength()), page.getPageType());
                randomAccessFile.setLength(Page.PAGE_SIZE * (pageNumber2 + 1));
                if (location == page.getNumberOfCells()) {
                    Page<DataRecord> page1 = new Page<>(pageNumber1);
                    page1.setPageType(page.getPageType());
                    page1.setNumberOfCells(page.getNumberOfCells());
                    page1.setRightNodeAddress(pageNumber2);
                    page1.setStartingAddress(page.getStartingAddress());
                    page1.setRecordAddressList(page.getRecordAddressList());
                    this.writePageHeader(randomAccessFile, page1);
                    List<DataRecord> records = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) 0, page.getNumberOfCells(), page1.getPageNumber(), record);
                    for (DataRecord object : records) {
                        this.writeRecord(randomAccessFile, object);
                    }
                    Page<DataRecord> page2 = new Page<>(pageNumber2);
                    page2.setPageType(page.getPageType());
                    page2.setNumberOfCells((byte) 1);
                    page2.setRightNodeAddress(page.getRightNodeAddress());
                    page2.setStartingAddress((short) (page2.getStartingAddress() - record.getSize() - record.getHeaderSize()));
                    page2.getRecordAddressList().add((short) (page2.getStartingAddress() + 1));
                    this.writePageHeader(randomAccessFile, page2);
                    record.setPageLocated(page2.getPageNumber());
                    record.setOffset((short) (page2.getStartingAddress() + 1));
                    this.writeRecord(randomAccessFile, record);
                    pointerRecord.setKey(record.getRowId());
                } else {
                    boolean isFirst = false;
                    if (location < (page.getRecordAddressList().size() / 2)) {
                        isFirst = true;
                    }
                    randomAccessFile.setLength(Page.PAGE_SIZE * (pageNumber2 + 1));

  
                    Page<DataRecord> page1 = new Page<>(pageNumber1);
                    page1.setPageType(page.getPageType());
                    page1.setPageNumber(pageNumber1);
                    List<DataRecord> leftRecords = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) 0, (byte) (page.getNumberOfCells() / 2), page1.getPageNumber(), record);
                    if (isFirst)
                        leftRecords.add(location, record);
                    page1.setNumberOfCells((byte) leftRecords.size());
                    int index = 0;
                    short offset = (short) (Page.PAGE_SIZE - 1);
                    for (DataRecord dataRecord : leftRecords) {
                        index++;
                        offset = (short) (Page.PAGE_SIZE - ((dataRecord.getSize() + dataRecord.getHeaderSize()) * index));
                        dataRecord.setOffset(offset);
                        page1.getRecordAddressList().add(offset);
                    }
                    page1.setStartingAddress((short) (offset + 1));
                    page1.setRightNodeAddress(pageNumber2);
                    this.writePageHeader(randomAccessFile, page1);
                    for(DataRecord dataRecord : leftRecords) {
                        this.writeRecord(randomAccessFile, dataRecord);
                    }

                 
                    Page<DataRecord> page2 = new Page<>(pageNumber2);
                    page2.setPageType(page.getPageType());
                    List<DataRecord> rightRecords = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) ((page.getNumberOfCells() / 2) + 1), page.getNumberOfCells(), pageNumber2, record);
                    if(!isFirst) {
                        int position = (location - (page.getRecordAddressList().size() / 2) + 1);
                        if(position >= rightRecords.size())
                            rightRecords.add(record);
                        else
                            rightRecords.add(position, record);
                    }
                    page2.setNumberOfCells((byte) rightRecords.size());
                    page2.setRightNodeAddress(page.getRightNodeAddress());
                    pointerRecord.setKey(rightRecords.get(0).getRowId());
                    index = 0;
                    offset = (short) (Page.PAGE_SIZE - 1);
                    for(DataRecord dataRecord : rightRecords) {
                        index++;
                        offset = (short) (Page.PAGE_SIZE - ((dataRecord.getSize() + dataRecord.getHeaderSize()) * index));
                        dataRecord.setOffset(offset);
                        page2.getRecordAddressList().add(offset);
                    }
                    page2.setStartingAddress((short) (offset + 1));
                    this.writePageHeader(randomAccessFile, page2);
                    for(DataRecord dataRecord : rightRecords) {
                        this.writeRecord(randomAccessFile, dataRecord);
                    }
                }
                pointerRecord.setLeftPageNumber(pageNumber1);
                return pointerRecord;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	private PointerRecord splitPage(RandomAccessFile randomAccessFile, Page page, DataRecord record) {
        if(page.getPageType() == Page.INTERIOR_TABLE_PAGE) {
            int pageNumber = binarySearch(randomAccessFile, record.getRowId(), page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.INTERIOR_TABLE_PAGE);
            Page newPage = this.readPageHeader(randomAccessFile, pageNumber);
            PointerRecord pointerRecord = splitPage(randomAccessFile, newPage, record);
            if(pointerRecord.getPageNumber() == -1)
                return pointerRecord;
            if(checkSpaceRequirements(page, pointerRecord)) {
                int location = binarySearch(randomAccessFile, record.getRowId(), page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.INTERIOR_TABLE_PAGE, true);
                page.setNumberOfCells((byte) (page.getNumberOfCells() + 1));
                page.setStartingAddress((short) (page.getStartingAddress() - pointerRecord.getSize()));
                page.getRecordAddressList().add(location, (short)(page.getStartingAddress() + 1));
                page.setRightNodeAddress(pointerRecord.getPageNumber());
                pointerRecord.setPageNumber(page.getPageNumber());
                pointerRecord.setOffset((short) (page.getStartingAddress() + 1));
                this.writePageHeader(randomAccessFile, page);
                this.writeRecord(randomAccessFile, pointerRecord);
                return new PointerRecord();
            }
            else {
                int newPageNumber;
                try {
                    newPageNumber = (int) (randomAccessFile.length() / Page.PAGE_SIZE);
                }
                catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
                page.setRightNodeAddress(pointerRecord.getPageNumber());
                this.writePageHeader(randomAccessFile, page);
                PointerRecord pointerRecord1;
                pointerRecord1 = splitPage(randomAccessFile, page, pointerRecord, page.getPageNumber(), newPageNumber);
                return pointerRecord1;
            }
        }
        else if(page.getPageType() == Page.LEAF_TABLE_PAGE) {
            int newPageNumber;
            try {
                newPageNumber = (int) (randomAccessFile.length() / Page.PAGE_SIZE);
            }
            catch (IOException e) {
                e.printStackTrace();
                return null;
            }
            PointerRecord pointerRecord = splitPage(randomAccessFile, page, record, page.getPageNumber(), newPageNumber);
            if(pointerRecord != null)
                pointerRecord.setPageNumber(newPageNumber);
            return pointerRecord;
        }
        return null;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	private PointerRecord splitPage(RandomAccessFile randomAccessFile, Page page, PointerRecord record, int pageNumber1, int pageNumber2) {
        try {
            if (page != null && record != null) {
                int location;
                boolean isFirst = false;

                PointerRecord pointerRecord;
                if(page.getPageType() == Page.LEAF_TABLE_PAGE) {
                    return null;
                }
                location = binarySearch(randomAccessFile, record.getKey(), page.getNumberOfCells(), ((page.getPageNumber() * Page.PAGE_SIZE) + Page.getHeaderFixedLength()), page.getPageType(), true);
                if (location < (page.getRecordAddressList().size() / 2)) {
                    isFirst = true;
                }

                if(pageNumber1 == 0) {
                    pageNumber1 = pageNumber2;
                    pageNumber2++;
                }
                randomAccessFile.setLength(Page.PAGE_SIZE * (pageNumber2 + 1));

            
                Page<PointerRecord> page1 = new Page<>(pageNumber1);
                page1.setPageType(page.getPageType());
                page1.setPageNumber(pageNumber1);
                List<PointerRecord> leftRecords = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) 0, (byte) (page.getNumberOfCells() / 2), page1.getPageNumber(), record);
                if (isFirst)
                    leftRecords.add(location, record);
                pointerRecord = leftRecords.get(leftRecords.size() - 1);
                pointerRecord.setPageNumber(pageNumber2);
                leftRecords.remove(leftRecords.size() - 1);
                page1.setNumberOfCells((byte) leftRecords.size());
                int index = 0;
                short offset = (short) (Page.PAGE_SIZE - 1);
                for (PointerRecord pointerRecord1 : leftRecords) {
                    index++;
                    offset = (short) (Page.PAGE_SIZE - (pointerRecord1.getSize() * index));
                    pointerRecord1.setOffset(offset);
                    page1.getRecordAddressList().add(offset);
                }
                page1.setStartingAddress((short) (offset + 1));
                page1.setRightNodeAddress(pointerRecord.getLeftPageNumber());
                this.writePageHeader(randomAccessFile, page1);
                for(PointerRecord pointerRecord1 : leftRecords) {
                    this.writeRecord(randomAccessFile, pointerRecord1);
                }

        
                Page<PointerRecord> page2 = new Page<>(pageNumber2);
                page2.setPageType(page.getPageType());
                List<PointerRecord> rightRecords = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) ((page.getNumberOfCells() / 2) + 1), page.getNumberOfCells(), pageNumber2, record);
                if(!isFirst) {
                    int position = (location - (page.getRecordAddressList().size() / 2) + 1);
                    if(position >= rightRecords.size())
                        rightRecords.add(record);
                    else
                        rightRecords.add(position, record);
                }
                page2.setNumberOfCells((byte) rightRecords.size());
                page2.setRightNodeAddress(page.getRightNodeAddress());
                rightRecords.get(0).setLeftPageNumber(page.getRightNodeAddress());
                index = 0;
                offset = (short) (Page.PAGE_SIZE - 1);
                for(PointerRecord pointerRecord1 : rightRecords) {
                    index++;
                    offset = (short) (Page.PAGE_SIZE - (pointerRecord1.getSize() * index));
                    pointerRecord1.setOffset(offset);
                    page2.getRecordAddressList().add(offset);
                }
                page2.setStartingAddress((short) (offset + 1));
                this.writePageHeader(randomAccessFile, page2);
                for(PointerRecord pointerRecord1 : rightRecords) {
                    this.writeRecord(randomAccessFile, pointerRecord1);
                }
                pointerRecord.setPageNumber(pageNumber2);
                pointerRecord.setLeftPageNumber(pageNumber1);
                return pointerRecord;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @SuppressWarnings("unchecked")
	private <T> List<T> copyRecords(RandomAccessFile randomAccessFile, long pageStartAddress, List<Short> recordAddresses, byte startIndex, byte endIndex, int pageNumber, T object) {
        try {
            List<T> records = new ArrayList<>();
            byte numberOfRecords;
            byte[] serialTypeCodes;
            for (byte i = startIndex; i < endIndex; i++) {
                randomAccessFile.seek(pageStartAddress + recordAddresses.get(i));
                if (object.getClass().equals(PointerRecord.class)) {
                    PointerRecord record = new PointerRecord();
                    record.setPageNumber(pageNumber);
                    record.setOffset((short) (pageStartAddress + Page.PAGE_SIZE - 1 - (record.getSize() * (i - startIndex + 1))));
                    record.setLeftPageNumber(randomAccessFile.readInt());
                    record.setKey(randomAccessFile.readInt());
                    records.add(i - startIndex, (T) record);
                } else if (object.getClass().equals(DataRecord.class)) {
                    DataRecord record = new DataRecord();
                    record.setPageLocated(pageNumber);
                    record.setOffset(recordAddresses.get(i));
                    record.setSize(randomAccessFile.readShort());
                    record.setRowId(randomAccessFile.readInt());
                    numberOfRecords = randomAccessFile.readByte();
                    serialTypeCodes = new byte[numberOfRecords];
                    for (byte j = 0; j < numberOfRecords; j++) {
                        serialTypeCodes[j] = randomAccessFile.readByte();
                    }
                    for (byte j = 0; j < numberOfRecords; j++) {
                        switch (serialTypeCodes[j]) {
                            case Constants.ONE_BYTE_NULL_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Text(null));
                                break;

                            case Constants.TWO_BYTE_NULL_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_SmallInt(randomAccessFile.readShort(), true));
                                break;

                            case Constants.FOUR_BYTE_NULL_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Real(randomAccessFile.readFloat(), true));
                                break;

                            case Constants.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Double(randomAccessFile.readDouble(), true));
                                break;

                            case Constants.TINY_INT_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_TinyInt(randomAccessFile.readByte()));
                                break;

                            case Constants.SMALL_INT_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_SmallInt(randomAccessFile.readShort()));
                                break;

                            case Constants.INT_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Int(randomAccessFile.readInt()));
                                break;

                            case Constants.BIG_INT_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DataTypeInt(randomAccessFile.readLong()));
                                break;

                            case Constants.REAL_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Real(randomAccessFile.readFloat()));
                                break;

                            case Constants.DOUBLE_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Double(randomAccessFile.readDouble()));
                                break;

                            case Constants.DATE_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Date(randomAccessFile.readLong()));
                                break;

                            case Constants.DATE_TIME_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_DateTime(randomAccessFile.readLong()));
                                break;

                            case Constants.TEXT_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Text(""));
                                break;

                            default:
                                if (serialTypeCodes[j] > Constants.TEXT_SERIAL_TYPE_CODE) {
                                    byte length = (byte) (serialTypeCodes[j] - Constants.TEXT_SERIAL_TYPE_CODE);
                                    char[] text = new char[length];
                                    for (byte k = 0; k < length; k++) {
                                        text[k] = (char) randomAccessFile.readByte();
                                    }
                                    record.getColumnValueList().add(new DT_Text(new String(text)));
                                }
                                break;

                        }
                    }
                    records.add(i - startIndex, (T) record);
                }
            }
            return records;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @SuppressWarnings("rawtypes")
	private Page getPage(RandomAccessFile randomAccessFile, DataRecord record, int pageNumber) {
        try {
            Page page = readPageHeader(randomAccessFile, pageNumber);
            if (page.getPageType() == Page.LEAF_TABLE_PAGE) {
                return page;
            }
            pageNumber = binarySearch(randomAccessFile, record.getRowId(), page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.INTERIOR_TABLE_PAGE);
            if (pageNumber == -1) return null;
            return getPage(randomAccessFile, record, pageNumber);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @SuppressWarnings("rawtypes")
	private int getAddress(File file, int rowId, int pageNumber) {
        int location = -1;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, pageNumber);
            if(page.getPageType() == Page.LEAF_TABLE_PAGE) {
                location = binarySearch(randomAccessFile, rowId, page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.LEAF_TABLE_PAGE);
                randomAccessFile.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return location;
    }

    private int binarySearch(RandomAccessFile randomAccessFile, int key, int numberOfRecords, long seekPosition, byte pageType) {
        return binarySearch(randomAccessFile, key, numberOfRecords, seekPosition, pageType, false);
    }

    private int binarySearch(RandomAccessFile randomAccessFile, int key, int numberOfRecords, long seekPosition, byte pageType, boolean literalSearch) {
        try {
            int start = 0, end = numberOfRecords;
            int mid;
            int pageNumber = -1;
            int rowId;
            short address;

            while(true) {
                if(start > end || start == numberOfRecords) {
                    if(pageType == Page.LEAF_TABLE_PAGE || literalSearch)
                        return start > numberOfRecords ? numberOfRecords : start;
                    if(pageType == Page.INTERIOR_TABLE_PAGE) {
                        if (end < 0)
                            return pageNumber;
                        randomAccessFile.seek(seekPosition - Page.getHeaderFixedLength() + 4);
                        return randomAccessFile.readInt();
                    }
                }
                mid = (start + end) / 2;
                randomAccessFile.seek(seekPosition + (Short.BYTES * mid));
                address = randomAccessFile.readShort();
                randomAccessFile.seek(seekPosition - Page.getHeaderFixedLength() + address);
                if (pageType == Page.LEAF_TABLE_PAGE) {
                    randomAccessFile.readShort();
                    rowId = randomAccessFile.readInt();
                    if (rowId == key) return mid;
                    if (rowId > key) {
                        end = mid - 1;
                    } else {
                        start = mid + 1;
                    }
                } else if (pageType == Page.INTERIOR_TABLE_PAGE) {
                    pageNumber = randomAccessFile.readInt();
                    rowId = randomAccessFile.readInt();
                    if (rowId > key) {
                        end = mid - 1;
                    } else {
                        start = mid + 1;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	private Page readPageHeader(RandomAccessFile randomAccessFile, int pageNumber) {
        try {
            Page page;
            randomAccessFile.seek(Page.PAGE_SIZE * pageNumber);
            byte pageType = randomAccessFile.readByte();
            if (pageType == Page.INTERIOR_TABLE_PAGE) {
                page = new Page<PointerRecord>();
            } else {
                page = new Page<DataRecord>();
            }
            page.setPageType(pageType);
            page.setPageNumber(pageNumber);
            page.setNumberOfCells(randomAccessFile.readByte());
            page.setStartingAddress(randomAccessFile.readShort());
            page.setRightNodeAddress(randomAccessFile.readInt());
            for (byte i = 0; i < page.getNumberOfCells(); i++) {
                page.getRecordAddressList().add(randomAccessFile.readShort());
            }
            return page;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @SuppressWarnings("rawtypes")
	private boolean writePageHeader(RandomAccessFile randomAccessFile, Page page) {
        try {System.out.println(page.getPageNumber() * Page.PAGE_SIZE);
            randomAccessFile.seek(page.getPageNumber() * Page.PAGE_SIZE);
            randomAccessFile.writeByte(page.getPageType());
            randomAccessFile.writeByte(page.getNumberOfCells());
            randomAccessFile.writeShort(page.getStartingAddress());
            randomAccessFile.writeInt(page.getRightNodeAddress());
            for (Object offset : page.getRecordAddressList()) {
                randomAccessFile.writeShort((short) offset);
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean writeRecord(RandomAccessFile randomAccessFile, DataRecord record) {
        try {
            randomAccessFile.seek((record.getPageLocated() * Page.PAGE_SIZE) + record.getOffset());
            randomAccessFile.writeShort(record.getSize());
            randomAccessFile.writeInt(record.getRowId());
            randomAccessFile.writeByte((byte) record.getColumnValueList().size());
            randomAccessFile.write(record.getSerialTypeCodes());
            for (Object object : record.getColumnValueList()) {
                switch (Utils.resolveClass(object)) {
                    case Constants.TINYINT:
                        randomAccessFile.writeByte(((DT_TinyInt) object).getValue());
                        break;

                    case Constants.SMALLINT:
                        randomAccessFile.writeShort(((DT_SmallInt) object).getValue());
                        break;

                    case Constants.INT:
                        randomAccessFile.writeInt(((DT_Int) object).getValue());
                        break;

                    case Constants.BIGINT:
                        randomAccessFile.writeLong(((DataTypeInt) object).getValue());
                        break;

                    case Constants.REAL:
                        randomAccessFile.writeFloat(((DT_Real) object).getValue());
                        break;

                    case Constants.DOUBLE:
                        randomAccessFile.writeDouble(((DT_Double) object).getValue());
                        break;

                    case Constants.DATE:
                        randomAccessFile.writeLong(((DT_Date) object).getValue());
                        break;

                    case Constants.DATETIME:
                        randomAccessFile.writeLong(((DT_DateTime) object).getValue());
                        break;

                    case Constants.TEXT:
                        if (((DT_Text) object).getValue() != null)
                            randomAccessFile.writeBytes(((DT_Text) object).getValue());
                        break;

                    default:
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean writeRecord(RandomAccessFile randomAccessFile, PointerRecord record) {
        try {
            randomAccessFile.seek((record.getPageNumber() * Page.PAGE_SIZE) + record.getOffset());
            randomAccessFile.writeInt(record.getLeftPageNumber());
            randomAccessFile.writeInt(record.getKey());
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public List<DataRecord> findRecord(String databaseName, String tableName, List<Byte> columnIndexList, List<Object> valueList, List<Short> conditionList, boolean getOne) {
        return findRecord(databaseName, tableName, columnIndexList, valueList, conditionList, null, getOne);
    }

    public List<DataRecord> findRecord(String databaseName, String tableName, List<Byte> columnIndexList, List<Object> valueList, List<Short> conditionList, List<Byte> selectionColumnIndexList, boolean getOne) {
        List<InternalCondition> conditions = new ArrayList<>();
        for (byte i = 0; i < columnIndexList.size(); i++) {
            conditions.add(InternalCondition.CreateCondition(columnIndexList.get(i), conditionList.get(i), valueList.get(i)));
        }
        return findRecord(databaseName, tableName, conditions, selectionColumnIndexList, getOne);
    }

    public List<DataRecord> findRecord(String databaseName, String tableName, InternalCondition condition, boolean getOne) {
        return findRecord(databaseName, tableName, condition,null, getOne);
    }

    public List<DataRecord> findRecord(String databaseName, String tableName, InternalCondition condition, List<Byte> selectionColumnIndexList, boolean getOne) {
        List<InternalCondition> conditionList = new ArrayList<>();
        if(condition != null)
            conditionList.add(condition);
        return findRecord(databaseName, tableName, conditionList, selectionColumnIndexList, getOne);
    }

    public List<DataRecord> findRecord(String databaseName, String tableName, List<InternalCondition> conditionList, boolean getOne) {
        return findRecord(databaseName, tableName, conditionList, null, getOne);
    }

    @SuppressWarnings({ "resource", "rawtypes" })
	public List<DataRecord> findRecord(String databaseName, String tableName, List<InternalCondition> conditionList, List<Byte> selectionColumnIndexList, boolean getOne) {
        try {
            File file = new File(databaseName + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                if (conditionList != null) {
                    Page page = getFirstPage(file);
                    DataRecord record;
                    List<DataRecord> matchRecords = new ArrayList<>();
                    boolean isMatch = false;
                    byte columnIndex;
                    short condition;
                    Object value;
                    while (page != null) {
                        for (Object offset : page.getRecordAddressList()) {
                            isMatch = true;
                            record = getDataRecord(randomAccessFile, page.getPageNumber(), (short) offset);
                            for(int i = 0; i < conditionList.size(); i++) {
                                isMatch = false;
                                columnIndex = conditionList.get(i).getIndex();
                                value = conditionList.get(i).getValue();
                                condition = conditionList.get(i).getConditionType();
                                if (record != null && record.getColumnValueList().size() > columnIndex) {
                                    Object object = record.getColumnValueList().get(columnIndex);
                                    switch (Utils.resolveClass(object)) {
                                        case Constants.TINYINT:
                                            switch (Utils.resolveClass(value)) {
                                                case Constants.TINYINT:
                                                    isMatch = ((DT_TinyInt) object).compare((DT_TinyInt) value, condition);
                                                    break;

                                                case Constants.SMALLINT:
                                                    isMatch = ((DT_TinyInt) object).compare((DT_SmallInt) value, condition);
                                                    break;

                                                case Constants.INT:
                                                    isMatch = ((DT_TinyInt) object).compare((DT_Int) value, condition);
                                                    break;

                                                case Constants.BIGINT:
                                                    isMatch = ((DT_TinyInt) object).compare((DataTypeInt) value, condition);
                                                    break;
                                            }
                                            break;

                                        case Constants.SMALLINT:
                                            switch (Utils.resolveClass(value)) {
                                                case Constants.TINYINT:
                                                    isMatch = ((DT_SmallInt) object).compare((DT_TinyInt) value, condition);
                                                    break;

                                                case Constants.SMALLINT:
                                                    isMatch = ((DT_SmallInt) object).compare((DT_SmallInt) value, condition);
                                                    break;

                                                case Constants.INT:
                                                    isMatch = ((DT_SmallInt) object).compare((DT_Int) value, condition);
                                                    break;

                                                case Constants.BIGINT:
                                                    isMatch = ((DT_SmallInt) object).compare((DataTypeInt) value, condition);
                                                    break;
                                            }
                                            break;

                                        case Constants.INT:
                                            switch (Utils.resolveClass(value)) {
                                                case Constants.TINYINT:
                                                    isMatch = ((DT_Int) object).compare((DT_TinyInt) value, condition);
                                                    break;

                                                case Constants.SMALLINT:
                                                    isMatch = ((DT_Int) object).compare((DT_SmallInt) value, condition);
                                                    break;

                                                case Constants.INT:
                                                    isMatch = ((DT_Int) object).compare((DT_Int) value, condition);
                                                    break;

                                                case Constants.BIGINT:
                                                    isMatch = ((DT_Int) object).compare((DataTypeInt) value, condition);
                                                    break;
                                            }
                                            break;

                                        case Constants.BIGINT:
                                            switch (Utils.resolveClass(value)) {
                                                case Constants.TINYINT:
                                                    isMatch = ((DataTypeInt) object).compare((DT_TinyInt) value, condition);
                                                    break;

                                                case Constants.SMALLINT:
                                                    isMatch = ((DataTypeInt) object).compare((DT_SmallInt) value, condition);
                                                    break;

                                                case Constants.INT:
                                                    isMatch = ((DataTypeInt) object).compare((DT_Int) value, condition);
                                                    break;

                                                case Constants.BIGINT:
                                                    isMatch = ((DataTypeInt) object).compare((DataTypeInt) value, condition);
                                                    break;
                                            }
                                            break;

                                        case Constants.REAL:
                                            switch (Utils.resolveClass(value)) {
                                                case Constants.REAL:
                                                    isMatch = ((DT_Real) object).compare((DT_Real) value, condition);
                                                    break;

                                                case Constants.DOUBLE:
                                                    isMatch = ((DT_Real) object).compare((DT_Double) value, condition);
                                                    break;
                                            }
                                            break;

                                        case Constants.DOUBLE:
                                            switch (Utils.resolveClass(value)) {
                                                case Constants.REAL:
                                                    isMatch = ((DT_Double) object).compare((DT_Real) value, condition);
                                                    break;

                                                case Constants.DOUBLE:
                                                    isMatch = ((DT_Double) object).compare((DT_Double) value, condition);
                                                    break;
                                            }
                                            break;

                                        case Constants.DATE:
                                            isMatch = ((DT_Date) object).compare((DT_Date) value, condition);
                                            break;

                                        case Constants.DATETIME:
                                            isMatch = ((DT_DateTime) object).compare((DT_DateTime) value, condition);
                                            break;

                                        case Constants.TEXT:
                                            if(((DT_Text) object).getValue() != null)
                                                isMatch = ((DT_Text) object).getValue().equalsIgnoreCase(((DT_Text) value).getValue());
                                            break;
                                    }
                                    if(!isMatch) break;
                                }
                            }

                            if(isMatch) {
                                DataRecord matchedRecord = record;
                                if(selectionColumnIndexList != null) {
                                    matchedRecord = new DataRecord();
                                    matchedRecord.setRowId(record.getRowId());
                                    matchedRecord.setPageLocated(record.getPageLocated());
                                    matchedRecord.setOffset(record.getOffset());
                                    for (Byte index : selectionColumnIndexList) {
                                        matchedRecord.getColumnValueList().add(record.getColumnValueList().get(index));
                                    }
                                }
                                matchRecords.add(matchedRecord);
                                if(getOne) {
                                    randomAccessFile.close();
                                    return matchRecords;
                                }
                            }
                        }
                        if (page.getRightNodeAddress() == Page.RIGHTMOST_PAGE)
                            break;
                        page = readPageHeader(randomAccessFile, page.getRightNodeAddress());
                    }
                    randomAccessFile.close();
                    return matchRecords;
                }
            } else {
                ConsoleWriter.displayMessage("Table " + tableName + " does not exist");
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public int updateRecord(String databaseName, String tableName, List<Byte> searchColumnIndexList, List<Object> searchValueList, List<Short> searchConditionList, List<Byte> updateColumnIndexList, List<Object> updateColumnValueList, boolean isIncrement) {
        List<InternalCondition> conditions = new ArrayList<>();
        for (byte i = 0; i < searchColumnIndexList.size(); i++) {
            conditions.add(InternalCondition.CreateCondition(searchColumnIndexList.get(i), searchConditionList.get(i), searchValueList.get(i)));
        }
        return updateRecord(databaseName, tableName, conditions, updateColumnIndexList, updateColumnValueList, isIncrement);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	public int updateRecord(String databaseName, String tableName, List<InternalCondition> conditions, List<Byte> updateColumnIndexList, List<Object> updateColumnValueList, boolean isIncrement) {
        int updateRecordCount = 0;
        try {
            if (conditions == null || updateColumnIndexList == null
                    || updateColumnValueList == null)
                return updateRecordCount;
            if (updateColumnIndexList.size() != updateColumnValueList.size())
                return updateRecordCount;
            File file = new File(databaseName + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                List<DataRecord> records = findRecord(databaseName, tableName, conditions, false);
                if (records != null) {
                    if (records.size() > 0) {
                        byte index;
                        Object object;
                        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                        for (DataRecord record : records) {
                            for (int i = 0; i < updateColumnIndexList.size(); i++) {
                                index = updateColumnIndexList.get(i);
                                object = updateColumnValueList.get(i);
                                if (isIncrement) {
                                    record.getColumnValueList().set(index, increment((DT_Numeric) record.getColumnValueList().get(index), (DT_Numeric) object));
                                } else {
                                    record.getColumnValueList().set(index, object);
                                }
                            }
                            this.writeRecord(randomAccessFile, record);
                            updateRecordCount++;
                        }
                        randomAccessFile.close();
                        return updateRecordCount;
                    }
                }
            } else {
                ConsoleWriter.displayMessage("Table " + tableName + " does not exist!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return updateRecordCount;
    }

    private <T> DT_Numeric<T> increment(DT_Numeric<T> object1, DT_Numeric<T> object2) {
        object1.increment(object2.getValue());
        return object1;
    }

    @SuppressWarnings("unchecked")
	public Page<DataRecord> getLastRecordAndPage(String databaseName, String tableName) {
        try {
            File file = new File(databaseName + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                Page<DataRecord> page = getLastPage(file);
                if (page.getNumberOfCells() > 0) {
                    randomAccessFile.seek((Page.PAGE_SIZE * page.getPageNumber()) + Page.getHeaderFixedLength() + ((page.getNumberOfCells() - 1) * Short.BYTES));
                    short address = randomAccessFile.readShort();
                    DataRecord record = getDataRecord(randomAccessFile, page.getPageNumber(), address);
                    if (record != null)
                        page.getPageRecords().add(record);
                }
                randomAccessFile.close();
                return page;
            } else {
                ConsoleWriter.displayMessage("File " + tableName + " does not exist");
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @SuppressWarnings("rawtypes")
	private Page getLastPage(File file) {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, 0);
            while (page.getPageType() == Page.INTERIOR_TABLE_PAGE && page.getRightNodeAddress() != Page.RIGHTMOST_PAGE) {
                page = readPageHeader(randomAccessFile, page.getRightNodeAddress());
            }
            randomAccessFile.close();
            return page;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @SuppressWarnings("rawtypes")
	private Page getFirstPage(File file) {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, 0);
            while (page.getPageType() == Page.INTERIOR_TABLE_PAGE) {
                if (page.getNumberOfCells() == 0) return null;
                randomAccessFile.seek((Page.PAGE_SIZE * page.getPageNumber()) + ((short) page.getRecordAddressList().get(0)));
                page = readPageHeader(randomAccessFile, randomAccessFile.readInt());
            }
            randomAccessFile.close();
            return page;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public int deleteRecord(String databaseName, String tableName, List<Byte> columnIndexList, List<Object> valueList, List<Short> conditionList) {
        return deleteRecord(databaseName, tableName, columnIndexList, valueList, conditionList, true);
    }

    @SuppressWarnings({ "resource", "rawtypes", "unchecked" })
	public int deleteRecord(String databaseName, String tableName, List<Byte> columnIndexList, List<Object> valueList, List<Short> conditionList, boolean deleteOne) {
        int deletedRecordCount = 0;
        try {
            File file = new File(databaseName + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                if(columnIndexList != null) {
                    Page page = getFirstPage(file);
                    DataRecord record;
                    boolean isMatch;
                    byte columnIndex;
                    short condition;
                    Object value;
                    while (page != null) {
                        for (Short offset : new ArrayList<Short>(page.getRecordAddressList())) {
                            isMatch = true;
                            record = getDataRecord(randomAccessFile, page.getPageNumber(), offset);
                            for(int i = 0; i < columnIndexList.size(); i++) {
                                isMatch = false;
                                columnIndex = columnIndexList.get(i);
                                value = valueList.get(i);
                                condition = conditionList.get(i);
                                if (record != null && record.getColumnValueList().size() > columnIndex) {
                                    Object object = record.getColumnValueList().get(columnIndex);
                                    switch (Utils.resolveClass(value)) {
                                        case Constants.TINYINT:
                                            isMatch = ((DT_TinyInt) value).compare((DT_TinyInt) object, condition);
                                            break;

                                        case Constants.SMALLINT:
                                            isMatch = ((DT_SmallInt) value).compare((DT_SmallInt) object, condition);
                                            break;

                                        case Constants.INT:
                                            isMatch = ((DT_Int) value).compare((DT_Int) object, condition);
                                            break;

                                        case Constants.BIGINT:
                                            isMatch = ((DataTypeInt) value).compare((DataTypeInt) object, condition);
                                            break;

                                        case Constants.REAL:
                                            isMatch = ((DT_Real) value).compare((DT_Real) object, condition);
                                            break;

                                        case Constants.DOUBLE:
                                            isMatch = ((DT_Double) value).compare((DT_Double) object, condition);
                                            break;

                                        case Constants.DATE:
                                            isMatch = ((DT_Date) value).compare((DT_Date) object, condition);
                                            break;

                                        case Constants.DATETIME:
                                            isMatch = ((DT_DateTime) value).compare((DT_DateTime) object, condition);
                                            break;

                                        case Constants.TEXT:
                                            isMatch = ((DT_Text) value).getValue().equalsIgnoreCase(((DT_Text) object).getValue());
                                            break;
                                    }
                                    if(!isMatch) break;
                                }
                            }
                            if(isMatch) {
                                page.setNumberOfCells((byte) (page.getNumberOfCells() - 1));
                                page.getRecordAddressList().remove(offset);
                                if(page.getNumberOfCells() == 0) {
                                    page.setStartingAddress((short) (page.getBaseAddress() + Page.PAGE_SIZE - 1));
                                }
                                this.writePageHeader(randomAccessFile, page);
                                UpdateStatementHelper.decrementRowCount(tableName);
                                deletedRecordCount++;
                                if(deleteOne) {
                                    randomAccessFile.close();
                                    return deletedRecordCount;
                                }
                            }
                        }
                        if(page.getRightNodeAddress() == Page.RIGHTMOST_PAGE)
                            break;
                        page = readPageHeader(randomAccessFile, page.getRightNodeAddress());
                    }
                    randomAccessFile.close();
                    return deletedRecordCount;
                }
            }
            else {
                ConsoleWriter.displayMessage("Table " + tableName + " does not exist");
                return deletedRecordCount;
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return deletedRecordCount;
    }

    public DataRecord getDataRecord(RandomAccessFile randomAccessFile, int pageNumber, short address) {
        return getDataRecord(randomAccessFile, pageNumber, address, null);
    }

    public DataRecord getDataRecord(RandomAccessFile randomAccessFile, int pageNumber, short address, List<Byte> columnList) {
        try {
            if (pageNumber >= 0 && address >= 0) {
                DataRecord record = new DataRecord();
                record.setPageLocated(pageNumber);
                record.setOffset(address);
                randomAccessFile.seek((Page.PAGE_SIZE * pageNumber) + address);
                record.setSize(randomAccessFile.readShort());
                record.setRowId(randomAccessFile.readInt());
                byte numberOfColumns = randomAccessFile.readByte();
                byte[] serialTypeCodes = new byte[numberOfColumns];
                for (byte i = 0; i < numberOfColumns; i++) {
                    serialTypeCodes[i] = randomAccessFile.readByte();
                }
                Object object;
                for (byte i = 0; i < numberOfColumns; i++) {
                    switch (serialTypeCodes[i]) {
                       
                        case Constants.ONE_BYTE_NULL_SERIAL_TYPE_CODE:
                            object = new DT_Text(null);
                            break;

                        case Constants.TWO_BYTE_NULL_SERIAL_TYPE_CODE:
                            object = new DT_SmallInt(randomAccessFile.readShort(), true);
                            break;

                        case Constants.FOUR_BYTE_NULL_SERIAL_TYPE_CODE:
                            object = new DT_Real(randomAccessFile.readFloat(), true);
                            break;

                        case Constants.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE:
                            object = new DT_Double(randomAccessFile.readDouble(), true);
                            break;

                        case Constants.TINY_INT_SERIAL_TYPE_CODE:
                            object = new DT_TinyInt(randomAccessFile.readByte());
                            break;

                        case Constants.SMALL_INT_SERIAL_TYPE_CODE:
                            object = new DT_SmallInt(randomAccessFile.readShort());
                            break;

                        case Constants.INT_SERIAL_TYPE_CODE:
                            object = new DT_Int(randomAccessFile.readInt());
                            break;

                        case Constants.BIG_INT_SERIAL_TYPE_CODE:
                            object = new DataTypeInt(randomAccessFile.readLong());
                            break;

                        case Constants.REAL_SERIAL_TYPE_CODE:
                            object = new DT_Real(randomAccessFile.readFloat());
                            break;

                        case Constants.DOUBLE_SERIAL_TYPE_CODE:
                            object = new DT_Double(randomAccessFile.readDouble());
                            break;

                        case Constants.DATE_SERIAL_TYPE_CODE:
                            object = new DT_Date(randomAccessFile.readLong());
                            break;

                        case Constants.DATE_TIME_SERIAL_TYPE_CODE:
                            object = new DT_DateTime(randomAccessFile.readLong());
                            break;

                        case Constants.TEXT_SERIAL_TYPE_CODE:
                            object = new DT_Text("");
                            break;

                        default:
                            if (serialTypeCodes[i] > Constants.TEXT_SERIAL_TYPE_CODE) {
                                byte length = (byte) (serialTypeCodes[i] - Constants.TEXT_SERIAL_TYPE_CODE);
                                char[] text = new char[length];
                                for (byte k = 0; k < length; k++) {
                                    text[k] = (char) randomAccessFile.readByte();
                                }
                                object = new DT_Text(new String(text));
                            } else
                                object = null;
                            break;
                    }
                    if (columnList != null && !columnList.contains(i)) continue;
                    record.getColumnValueList().add(object);
                }
                return record;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    
    @SuppressWarnings("rawtypes")
	public List<String> fetchAllTableColumns(String tableName) {
        List<String> columnNames = new ArrayList<>();
        List<Byte> columnIndexList = new ArrayList<>();
        columnIndexList.add(CatalogDB.COLUMNS_TABLE_SCHEMA_TABLE_NAME);

        List<Object> valueList = new ArrayList<>();
        valueList.add(new DT_Text(tableName));

        List<Short> conditionList = new ArrayList<>();
        conditionList.add(DT_Numeric.EQUALS);

        List<DataRecord> records = this.findRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_COLUMNS_TABLENAME, columnIndexList, valueList, conditionList, false);

        for (int i = 0; i < records.size(); i++) {
            DataRecord record = records.get(i);
            Object object = record.getColumnValueList().get(CatalogDB.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            columnNames.add(((DT) object).getStringValue());
        }

        return columnNames;
    }

    @SuppressWarnings("rawtypes")
	public boolean checkNullConstraint(String tableName, HashMap<String, Integer> columnMap) {

        List<Byte> columnIndexList = new ArrayList<>();
        columnIndexList.add(CatalogDB.COLUMNS_TABLE_SCHEMA_TABLE_NAME);

        List<Object> valueList = new ArrayList<>();
        valueList.add(new DT_Text(tableName));

        List<Short> conditionList = new ArrayList<>();
        conditionList.add(DT_Numeric.EQUALS);

        List<DataRecord> records = this.findRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_COLUMNS_TABLENAME, columnIndexList, valueList, conditionList, false);

        for (int i = 0; i < records.size(); i++) {
            DataRecord record = records.get(i);
            Object nullValueObject = record.getColumnValueList().get(CatalogDB.COLUMNS_TABLE_SCHEMA_IS_NULLABLE);
            Object object = record.getColumnValueList().get(CatalogDB.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);

            String isNullStr = ((DT) nullValueObject).getStringValue();
            boolean isNullable = (isNullStr.compareToIgnoreCase("NULL") == 0) ? false : true;
            if (isNullable) {
                isNullable = (isNullStr.compareToIgnoreCase("NO") == 0) ? true : false;
            }

            if (!columnMap.containsKey(((DT) object).getStringValue()) && isNullable) {
                Utils.printMessage("Field '" + ((DT) object).getStringValue() + "' doesn't have a default value");
                return false;
            }

        }

        return true;
    }

    @SuppressWarnings("rawtypes")
	public HashMap<String, Integer> fetchAllTableColumndataTypes(String tableName) {
        List<Byte> columnIndexList = new ArrayList<>();
        columnIndexList.add(CatalogDB.COLUMNS_TABLE_SCHEMA_TABLE_NAME);

        List<Object> valueList = new ArrayList<>();
        valueList.add(new DT_Text(tableName));

        List<Short> conditionList = new ArrayList<>();
        conditionList.add(DT_Numeric.EQUALS);

        List<DataRecord> records = this.findRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_COLUMNS_TABLENAME, columnIndexList, valueList, conditionList, false);
        HashMap<String, Integer> columDataTypeMapping = new HashMap<>();

        for (int i = 0; i < records.size(); i++) {
            DataRecord record = records.get(i);
            Object object = record.getColumnValueList().get(CatalogDB.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            Object dataTypeObject = record.getColumnValueList().get(CatalogDB.COLUMNS_TABLE_SCHEMA_DATA_TYPE);

            String columnName = ((DT) object).getStringValue();
            int columnDataType = Utils.stringToDataType(((DT) dataTypeObject).getStringValue());

            columDataTypeMapping.put(columnName.toLowerCase(), columnDataType);

        }

        return columDataTypeMapping;
    }

    @SuppressWarnings("rawtypes")
	public String getTablePrimaryKey(String tableName) {
        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(InternalCondition.CreateCondition(CatalogDB.COLUMNS_TABLE_SCHEMA_COLUMN_NAME, InternalCondition.EQUALS, new DT_Text(tableName)));
        conditions.add(InternalCondition.CreateCondition(CatalogDB.COLUMNS_TABLE_SCHEMA_COLUMN_KEY, InternalCondition.EQUALS, new DT_Text(CatalogDB.PRIMARY_KEY_IDENTIFIER)));

        List<DataRecord> records = this.findRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_COLUMNS_TABLENAME, conditions, false);
        String columnName = "";
        for (DataRecord record : records) {
            Object object = record.getColumnValueList().get(CatalogDB.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            columnName = ((DT) object).getStringValue();
            break;
        }

        return columnName;
    }

    @SuppressWarnings("rawtypes")
	public int getTableRecordCount(String tableName) {
        InternalCondition condition = InternalCondition.CreateCondition(CatalogDB.TABLES_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new DT_Text(tableName));

        List<DataRecord> records = this.findRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_TABLES_TABLENAME, condition, true);
        int recordCount = 0;

        for (DataRecord record : records) {
            Object object = record.getColumnValueList().get(CatalogDB.TABLES_TABLE_SCHEMA_RECORD_COUNT);
            recordCount = Integer.valueOf(((DT) object).getStringValue());
            break;
        }

        return recordCount;
    }

    public boolean checkIfValueForPrimaryKeyExists(String databaseName, String tableName, int value) {
        StorageManager manager = new StorageManager();
        InternalCondition condition = InternalCondition.CreateCondition(0, InternalCondition.EQUALS, new DT_Int(value));

        List<DataRecord> records = manager.findRecord(Utils.getUserDatabasePath(databaseName), tableName, condition, false);
        if (records.size() > 0) {
            return true;
        }
        else {
            return false;
        }
    }
}
