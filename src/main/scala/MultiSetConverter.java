import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.spark_project.guava.collect.HashMultiset;
import org.spark_project.guava.collect.Multiset;

public class MultiSetConverter<T> extends PrimitiveConverter {

  public String getColumnName() {
    return columnName;
  }

  public Multiset<T> getCount() {
    return count;
  }

  protected String columnName = null;

  protected Multiset<T> count = HashMultiset.create();

  public MultiSetConverter(String columnName){
    this.columnName = columnName;
  }

  @Override
  public void addBinary(Binary value) {
    count.add((T)value);
  }

  @Override
  public void addBoolean(boolean value) {
    count.add((T)Boolean.valueOf(value));
  }

  @Override
  public void addDouble(double value) {
    count.add((T)Double.valueOf(value));
  }

  @Override
  public void addFloat(float value) {
    count.add((T)Float.valueOf(value));
  }

  @Override
  public void addInt(int value) {
    count.add((T)Integer.valueOf(value));
  }

  @Override
  public void addLong(long value) {
    count.add((T)Long.valueOf(value));
  }
}

