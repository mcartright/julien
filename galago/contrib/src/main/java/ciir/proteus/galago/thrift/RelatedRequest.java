/**
 * Autogenerated by Thrift Compiler (0.8.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package ciir.proteus.galago.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelatedRequest implements org.apache.thrift.TBase<RelatedRequest, RelatedRequest._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("RelatedRequest");

  private static final org.apache.thrift.protocol.TField BELIEFS_FIELD_DESC = new org.apache.thrift.protocol.TField("beliefs", org.apache.thrift.protocol.TType.LIST, (short)1);
  private static final org.apache.thrift.protocol.TField TARGET_TYPES_FIELD_DESC = new org.apache.thrift.protocol.TField("target_types", org.apache.thrift.protocol.TType.LIST, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new RelatedRequestStandardSchemeFactory());
    schemes.put(TupleScheme.class, new RelatedRequestTupleSchemeFactory());
  }

  public List<SearchResult> beliefs; // required
  public List<ProteusType> target_types; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    BELIEFS((short)1, "beliefs"),
    TARGET_TYPES((short)2, "target_types");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // BELIEFS
          return BELIEFS;
        case 2: // TARGET_TYPES
          return TARGET_TYPES;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.BELIEFS, new org.apache.thrift.meta_data.FieldMetaData("beliefs", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, SearchResult.class))));
    tmpMap.put(_Fields.TARGET_TYPES, new org.apache.thrift.meta_data.FieldMetaData("target_types", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, ProteusType.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(RelatedRequest.class, metaDataMap);
  }

  public RelatedRequest() {
  }

  public RelatedRequest(
    List<SearchResult> beliefs,
    List<ProteusType> target_types)
  {
    this();
    this.beliefs = beliefs;
    this.target_types = target_types;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public RelatedRequest(RelatedRequest other) {
    if (other.isSetBeliefs()) {
      List<SearchResult> __this__beliefs = new ArrayList<SearchResult>();
      for (SearchResult other_element : other.beliefs) {
        __this__beliefs.add(new SearchResult(other_element));
      }
      this.beliefs = __this__beliefs;
    }
    if (other.isSetTarget_types()) {
      List<ProteusType> __this__target_types = new ArrayList<ProteusType>();
      for (ProteusType other_element : other.target_types) {
        __this__target_types.add(other_element);
      }
      this.target_types = __this__target_types;
    }
  }

  public RelatedRequest deepCopy() {
    return new RelatedRequest(this);
  }

  @Override
  public void clear() {
    this.beliefs = null;
    this.target_types = null;
  }

  public int getBeliefsSize() {
    return (this.beliefs == null) ? 0 : this.beliefs.size();
  }

  public java.util.Iterator<SearchResult> getBeliefsIterator() {
    return (this.beliefs == null) ? null : this.beliefs.iterator();
  }

  public void addToBeliefs(SearchResult elem) {
    if (this.beliefs == null) {
      this.beliefs = new ArrayList<SearchResult>();
    }
    this.beliefs.add(elem);
  }

  public List<SearchResult> getBeliefs() {
    return this.beliefs;
  }

  public RelatedRequest setBeliefs(List<SearchResult> beliefs) {
    this.beliefs = beliefs;
    return this;
  }

  public void unsetBeliefs() {
    this.beliefs = null;
  }

  /** Returns true if field beliefs is set (has been assigned a value) and false otherwise */
  public boolean isSetBeliefs() {
    return this.beliefs != null;
  }

  public void setBeliefsIsSet(boolean value) {
    if (!value) {
      this.beliefs = null;
    }
  }

  public int getTarget_typesSize() {
    return (this.target_types == null) ? 0 : this.target_types.size();
  }

  public java.util.Iterator<ProteusType> getTarget_typesIterator() {
    return (this.target_types == null) ? null : this.target_types.iterator();
  }

  public void addToTarget_types(ProteusType elem) {
    if (this.target_types == null) {
      this.target_types = new ArrayList<ProteusType>();
    }
    this.target_types.add(elem);
  }

  public List<ProteusType> getTarget_types() {
    return this.target_types;
  }

  public RelatedRequest setTarget_types(List<ProteusType> target_types) {
    this.target_types = target_types;
    return this;
  }

  public void unsetTarget_types() {
    this.target_types = null;
  }

  /** Returns true if field target_types is set (has been assigned a value) and false otherwise */
  public boolean isSetTarget_types() {
    return this.target_types != null;
  }

  public void setTarget_typesIsSet(boolean value) {
    if (!value) {
      this.target_types = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case BELIEFS:
      if (value == null) {
        unsetBeliefs();
      } else {
        setBeliefs((List<SearchResult>)value);
      }
      break;

    case TARGET_TYPES:
      if (value == null) {
        unsetTarget_types();
      } else {
        setTarget_types((List<ProteusType>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case BELIEFS:
      return getBeliefs();

    case TARGET_TYPES:
      return getTarget_types();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case BELIEFS:
      return isSetBeliefs();
    case TARGET_TYPES:
      return isSetTarget_types();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof RelatedRequest)
      return this.equals((RelatedRequest)that);
    return false;
  }

  public boolean equals(RelatedRequest that) {
    if (that == null)
      return false;

    boolean this_present_beliefs = true && this.isSetBeliefs();
    boolean that_present_beliefs = true && that.isSetBeliefs();
    if (this_present_beliefs || that_present_beliefs) {
      if (!(this_present_beliefs && that_present_beliefs))
        return false;
      if (!this.beliefs.equals(that.beliefs))
        return false;
    }

    boolean this_present_target_types = true && this.isSetTarget_types();
    boolean that_present_target_types = true && that.isSetTarget_types();
    if (this_present_target_types || that_present_target_types) {
      if (!(this_present_target_types && that_present_target_types))
        return false;
      if (!this.target_types.equals(that.target_types))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(RelatedRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    RelatedRequest typedOther = (RelatedRequest)other;

    lastComparison = Boolean.valueOf(isSetBeliefs()).compareTo(typedOther.isSetBeliefs());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBeliefs()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.beliefs, typedOther.beliefs);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTarget_types()).compareTo(typedOther.isSetTarget_types());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTarget_types()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.target_types, typedOther.target_types);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("RelatedRequest(");
    boolean first = true;

    sb.append("beliefs:");
    if (this.beliefs == null) {
      sb.append("null");
    } else {
      sb.append(this.beliefs);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("target_types:");
    if (this.target_types == null) {
      sb.append("null");
    } else {
      sb.append(this.target_types);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class RelatedRequestStandardSchemeFactory implements SchemeFactory {
    public RelatedRequestStandardScheme getScheme() {
      return new RelatedRequestStandardScheme();
    }
  }

  private static class RelatedRequestStandardScheme extends StandardScheme<RelatedRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, RelatedRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // BELIEFS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list146 = iprot.readListBegin();
                struct.beliefs = new ArrayList<SearchResult>(_list146.size);
                for (int _i147 = 0; _i147 < _list146.size; ++_i147)
                {
                  SearchResult _elem148; // optional
                  _elem148 = new SearchResult();
                  _elem148.read(iprot);
                  struct.beliefs.add(_elem148);
                }
                iprot.readListEnd();
              }
              struct.setBeliefsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TARGET_TYPES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list149 = iprot.readListBegin();
                struct.target_types = new ArrayList<ProteusType>(_list149.size);
                for (int _i150 = 0; _i150 < _list149.size; ++_i150)
                {
                  ProteusType _elem151; // optional
                  _elem151 = ProteusType.findByValue(iprot.readI32());
                  struct.target_types.add(_elem151);
                }
                iprot.readListEnd();
              }
              struct.setTarget_typesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, RelatedRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.beliefs != null) {
        oprot.writeFieldBegin(BELIEFS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.beliefs.size()));
          for (SearchResult _iter152 : struct.beliefs)
          {
            _iter152.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.target_types != null) {
        oprot.writeFieldBegin(TARGET_TYPES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I32, struct.target_types.size()));
          for (ProteusType _iter153 : struct.target_types)
          {
            oprot.writeI32(_iter153.getValue());
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class RelatedRequestTupleSchemeFactory implements SchemeFactory {
    public RelatedRequestTupleScheme getScheme() {
      return new RelatedRequestTupleScheme();
    }
  }

  private static class RelatedRequestTupleScheme extends TupleScheme<RelatedRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, RelatedRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetBeliefs()) {
        optionals.set(0);
      }
      if (struct.isSetTarget_types()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetBeliefs()) {
        {
          oprot.writeI32(struct.beliefs.size());
          for (SearchResult _iter154 : struct.beliefs)
          {
            _iter154.write(oprot);
          }
        }
      }
      if (struct.isSetTarget_types()) {
        {
          oprot.writeI32(struct.target_types.size());
          for (ProteusType _iter155 : struct.target_types)
          {
            oprot.writeI32(_iter155.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, RelatedRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list156 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.beliefs = new ArrayList<SearchResult>(_list156.size);
          for (int _i157 = 0; _i157 < _list156.size; ++_i157)
          {
            SearchResult _elem158; // optional
            _elem158 = new SearchResult();
            _elem158.read(iprot);
            struct.beliefs.add(_elem158);
          }
        }
        struct.setBeliefsIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list159 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I32, iprot.readI32());
          struct.target_types = new ArrayList<ProteusType>(_list159.size);
          for (int _i160 = 0; _i160 < _list159.size; ++_i160)
          {
            ProteusType _elem161; // optional
            _elem161 = ProteusType.findByValue(iprot.readI32());
            struct.target_types.add(_elem161);
          }
        }
        struct.setTarget_typesIsSet(true);
      }
    }
  }

}
