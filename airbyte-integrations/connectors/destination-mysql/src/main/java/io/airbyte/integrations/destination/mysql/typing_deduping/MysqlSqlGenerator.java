/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.mysql.typing_deduping;

import static io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_AB_EXTRACTED_AT;
import static io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_AB_LOADED_AT;
import static io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_AB_META;
import static io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_DATA;
import static org.jooq.impl.DSL.case_;
import static org.jooq.impl.DSL.cast;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.function;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.quotedName;
import static org.jooq.impl.DSL.rowNumber;
import static org.jooq.impl.DSL.trueCondition;
import static org.jooq.impl.DSL.val;

import io.airbyte.cdk.integrations.destination.NamingConventionTransformer;
import io.airbyte.cdk.integrations.destination.jdbc.TableDefinition;
import io.airbyte.cdk.integrations.destination.jdbc.typing_deduping.JdbcSqlGenerator;
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteProtocolType;
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteType;
import io.airbyte.integrations.base.destination.typing_deduping.Array;
import io.airbyte.integrations.base.destination.typing_deduping.ColumnId;
import io.airbyte.integrations.base.destination.typing_deduping.Sql;
import io.airbyte.integrations.base.destination.typing_deduping.StreamConfig;
import io.airbyte.integrations.base.destination.typing_deduping.StreamId;
import io.airbyte.integrations.base.destination.typing_deduping.Struct;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.jooq.Condition;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.SQLDialect;
import org.jooq.SortField;
import org.jooq.impl.DefaultDataType;
import org.jooq.impl.SQLDataType;

public class MysqlSqlGenerator extends JdbcSqlGenerator {

  public static final DefaultDataType<Object> JSON_TYPE = new DefaultDataType<>(null, Object.class, "json");

  public MysqlSqlGenerator(final NamingConventionTransformer namingResolver) {
    super(namingResolver);
  }

  private DataType<Object> getJsonType() {
    return JSON_TYPE;
  }

  @Override
  protected DataType<?> getStructType() {
    return getJsonType();
  }

  @Override
  protected DataType<?> getArrayType() {
    return getJsonType();
  }

  @Override
  public DataType<?> toDialectType(final AirbyteProtocolType airbyteProtocolType) {
    return switch (airbyteProtocolType) {
      // jooq's TIMESTMAPWITHTIMEZONE type renders to `timestamp with timezone`...
      // which isn't valid mysql syntax.
      // Legacy normalization used char(1024)
      // https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/bases/base-normalization/dbt-project-template/macros/cross_db_utils/datatypes.sql#L233-L234
      // so match that behavior I guess.
      case TIMESTAMP_WITH_TIMEZONE -> SQLDataType.VARCHAR(1024);
      // Mysql doesn't have a native time with timezone type.
      // Legacy normalization used char(1024)
      // https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/bases/base-normalization/dbt-project-template/macros/cross_db_utils/datatypes.sql#L233-L234
      // so match that behavior I guess.
      case TIME_WITH_TIMEZONE -> SQLDataType.VARCHAR(1024);
      // Mysql VARCHAR can only go up to 16KiB. CLOB translates to mysql TEXT,
      // which supports longer strings.
      case STRING -> SQLDataType.CLOB;
      default -> super.toDialectType(airbyteProtocolType);
    };
  }

  @Override
  protected DataType<?> getWidestType() {
    return getJsonType();
  }

  @Override
  protected SQLDialect getDialect() {
    return SQLDialect.MYSQL;
  }

  @Override
  protected List<Field<?>> extractRawDataFields(final LinkedHashMap<ColumnId, AirbyteType> columns, final boolean useExpensiveSaferCasting) {
    return columns
        .entrySet()
        .stream()
        .map(column -> {
          final AirbyteType type = column.getValue();
          final boolean isStruct = type instanceof Struct;
          final boolean isArray = type instanceof Array;

          Field<?> extractedValue = extractColumnAsJson(column.getKey());
          if (!(isStruct || isArray || type == AirbyteProtocolType.UNKNOWN)) {
            // Primitive types need to use JSON_VALUE to (a) strip quotes from strings, and
            // (b) cast json null to sql null.
            extractedValue = function("JSON_VALUE", String.class, extractedValue, val("$"));
          }
          if (isStruct) {
            return case_()
                .when(
                    extractedValue.isNull()
                        .or(function("JSON_TYPE", String.class, extractedValue).ne("OBJECT")),
                    val((Object) null))
                .else_(extractedValue)
                .as(quotedName(column.getKey().name()));
          } else if (isArray) {
            return case_()
                .when(
                    extractedValue.isNull()
                        .or(function("JSON_TYPE", String.class, extractedValue).ne("ARRAY")),
                    val((Object) null))
                .else_(extractedValue)
                .as(quotedName(column.getKey().name()));
          } else {
            final Field<?> castedValue = castedField(extractedValue, type, useExpensiveSaferCasting);
            if (!(type instanceof final AirbyteProtocolType primitive)) {
              return castedValue.as(quotedName(column.getKey().name()));
            }
            return switch (primitive) {
              // These types are just casting to strings, so we need to use regex to validate their format
              case TIME_WITH_TIMEZONE -> case_()
                  .when(castedValue.notLikeRegex("^[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?([-+][0-9]{2}:[0-9]{2}|Z)$"), val((Object) null))
                  .else_(castedValue)
                  .as(quotedName(column.getKey().name()));
              case TIMESTAMP_WITH_TIMEZONE -> case_()
                  .when(castedValue.notLikeRegex("^[0-9]+-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?([-+][0-9]{2}:[0-9]{2}|Z)$"),
                      val((Object) null))
                  .else_(castedValue)
                  .as(quotedName(column.getKey().name()));
              default -> castedValue.as(quotedName(column.getKey().name()));
            };
          }
        })
        .collect(Collectors.toList());
  }

  @Override
  protected Field<?> castedField(final Field<?> field, final AirbyteProtocolType type, final boolean useExpensiveSaferCasting) {
    if (type == AirbyteProtocolType.BOOLEAN) {
      // for some reason, CAST('true' AS UNSIGNED) throws an error
      // so we manually build a case statement to do the string equality check
      return case_()
          // The coerce just tells jooq that we're assuming `field` is a string value
          .when(field.coerce(String.class).eq(val("true")), val(true))
          .when(field.coerce(String.class).eq(val("false")), val(false))
          .else_(val((Boolean) null));
    } else {
      return cast(field, toDialectType(type));
    }
  }

  @Override
  protected Field<?> buildAirbyteMetaColumn(final LinkedHashMap<ColumnId, AirbyteType> columns) {
    // TODO destination-mysql does not currently support safe casting. Intentionally don't populate
    // the airbyte_meta.errors field.
    return cast(val("{}"), getJsonType()).as(COLUMN_NAME_AB_META);
  }

  @Override
  protected Condition cdcDeletedAtNotNullCondition() {
    return field(name(COLUMN_NAME_AB_LOADED_AT)).isNotNull()
        .and(jsonTypeof(extractColumnAsJson(cdcDeletedAtColumn)).ne("NULL"));
  }

  @Override
  protected Field<Integer> getRowNumber(final List<ColumnId> primaryKeys, final Optional<ColumnId> cursor) {
    final List<Field<?>> primaryKeyFields =
        primaryKeys != null ? primaryKeys.stream().map(columnId -> field(quotedName(columnId.name()))).collect(Collectors.toList())
            : new ArrayList<>();
    final List<SortField<Object>> orderedFields = new ArrayList<>();
    // mysql DESC implicitly sorts nulls last, so we don't need to specify it explicitly
    cursor.ifPresent(columnId -> orderedFields.add(field(quotedName(columnId.name())).desc()));
    orderedFields.add(field(quotedName(COLUMN_NAME_AB_EXTRACTED_AT)).desc());
    return rowNumber()
        .over()
        .partitionBy(primaryKeyFields)
        .orderBy(orderedFields).as(ROW_NUMBER_COLUMN_NAME);
  }

  @Override
  public Sql createSchema(final String schema) {
    throw new NotImplementedException();
  }

  @Override
  protected String renameTable(final StreamId stream, final String finalSuffix) {
    return getDslContext().alterTable(name(stream.finalNamespace(), stream.finalName() + finalSuffix))
        // mysql ALTER TABLE ... REANME TO requires a fully-qualified target table name.
        // otherwise it puts the table in the default database, which is typically not what we want.
        .renameTo(name(stream.finalNamespace(), stream.finalName()))
        .getSQL();
  }

  @Override
  public boolean existingSchemaMatchesStreamConfig(final StreamConfig stream, final TableDefinition existingTable) {
    throw new NotImplementedException();
  }

  @Override
  protected String beginTransaction() {
    return "START TRANSACTION";
  }

  private Field<Object> extractColumnAsJson(final ColumnId column) {
    return function("JSON_EXTRACT", getJsonType(), field(name(COLUMN_NAME_DATA)), jsonPath(column));
  }

  private Field<String> jsonTypeof(final Field<?> field) {
    return function("JSON_TYPE", SQLDataType.VARCHAR, field);
  }

  private static Param<String> jsonPath(final ColumnId column) {
    // TODO escape jsonpath
    // We wrap the name in doublequotes for special character handling, and then escape the quoted string.
    final String escapedName = column.originalName()
        .replace("\"", "\\\"")
        .replace("\\\\", "\\\\\\\\");
    return val("$.\"" + escapedName + "\"");
  }

}
