import java.util.HashMap;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.tuple.Tuple;

/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager{
  private HashMap<String, TableMetadata> table = new HashMap<>();
  public TableManagerImpl() {
    
  FDB fdb = FDB.selectAPIVersion(710);
        Database db = null;
        DirectorySubspace rootDirectory = null;
   try {
            db = fdb.open();
        } catch (Exception e) {
            System.out.println("ERROR: the database is not successfully opened: " + e);
        }

        System.out.println("Open FDB Successfully!");
        try {
            rootDirectory = DirectoryLayer.getDefault().createOrOpen(db,
                    PathUtil.from("Company")).join();
        } catch (Exception e) {
            System.out.println("ERROR: the root directory is not successfully opened: " + e);
        }
  }
  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType,
                         String[] primaryKeyAttributeNames) {
    // your code
    if (table.containsKey(tableName)) {
      return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
    }
    if (attributeNames == null  || attributeType == null) {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }
    if (primaryKeyAttributeNames == null) {
      return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;
    }
    for (TableMetadata t : table.values()) {
            if (Arrays.equals(t.getPrimaryKeyAttributeNames(), primaryKeyAttributeNames)) {
                return StatusCode.TABLE_CREATION_PRIMARY_KEY_ALREADY_EXISTS;
            }
        }
   table.put(tableName, new TableMetadata(attributeNames, attributeType, primaryKeyAttributeNames));
  

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    if (!table.containsKey(tableName)) {
            return StatusCode.TABLE_NOT_FOUND;
        }
    table.remove(tableName);
    return StatusCode.SUCCESS;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    // your code
    return new HashMap<>(table);
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {
    // your code
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    // your code
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAllTables() {
    // your code
    return StatusCode.SUCCESS;
  }
}
