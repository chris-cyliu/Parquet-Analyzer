import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

public class CountSetConverter extends PrimitiveConverter {

  public String getColumnName() {
    return columnName;
  }

  public CountSet getCount() {
    return count;
  }

  protected String columnName = null;

  protected CountSet count = new CountSet();

  public CountSetConverter(String columnName){
    this.columnName = columnName;
  }

  @Override
  public void addBinary(Binary value) {
    count.add(value.toString());
  }

  @Override
  public void addBoolean(boolean value) {
    count.add(Boolean.toString(value));
  }

  @Override
  public void addDouble(double value) {
    count.add(Double.toString(value));
  }

  @Override
  public void addFloat(float value) {
    count.add(Float.toString(value));
  }

  @Override
  public void addInt(int value) {
    count.add(Integer.toString(value));
  }

  @Override
  public void addLong(long value) {
    count.add(Long.toString(value));
  }
}

