/**
 *    Copyright (C) 2020-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/document_source_parquet.h"

namespace mongo {

REGISTER_DOCUMENT_SOURCE(parquet,
                         LiteParsedDocumentSourceDefault::parse,
                         DocumentSourceParquet::createFromBson);

boost::intrusive_ptr<DocumentSource> DocumentSourceParquet::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    uassert(
        6000000, "$parquet stage takes a single string argument", elem.type() == BSONType::String);
    try {
        return make_intrusive<DocumentSourceParquet>(expCtx, elem.str());
    } catch (...) {
        uasserted(600003, "Failed to open parquet file");
    }
}

DocumentSourceParquet::DocumentSourceParquet(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                             std::string fileName)
    : DocumentSource(kStageName, expCtx),
      _fileName(std::move(fileName)),
      _fileReader(parquet::ParquetFileReader::OpenFile(_fileName, false)),
      _totalRowGroups(_fileReader->metadata()->num_row_groups()) {
    initForNextRowGroup();
}

const char* DocumentSourceParquet::getSourceName() const {
    return kStageName.rawData();
}

void DocumentSourceParquet::initForNextRowGroup() {
    ++_curRowGroup;

    // If there is no next row group, then bail out, since there is no other state to initialize.
    if (_curRowGroup >= _totalRowGroups) {
        return;
    }

    invariant(_curRowGroup >= 0);
    invariant(_curRowGroup < _totalRowGroups);

    auto rowGroupReader = _fileReader->RowGroup(_curRowGroup);
    auto rowGroupMetadata = rowGroupReader->metadata();
    auto schemaDescriptor = rowGroupMetadata->schema();

    _totalRowsInGroup = rowGroupMetadata->num_rows();
    _curRow = 0;

    std::vector<ColumnInfo> newColumns;
    for (int i = 0; i < rowGroupMetadata->num_columns(); ++i) {
        newColumns.emplace_back(rowGroupReader->Column(i), *schemaDescriptor->Column(i));
    }
    _columns.swap(newColumns);
}

namespace {
template <typename T, typename ReaderType>
T readSingleColumnValue(parquet::ColumnReader* reader) {
    auto typedReader = static_cast<ReaderType*>(reader);

    T value;
    int64_t valuesRead;
    // TODO: Might have to deal with definition/repetition levels.
    auto rowsRead = typedReader->ReadBatch(1, nullptr, nullptr, &value, &valuesRead);

    invariant(rowsRead == 1);
    invariant(valuesRead == 1);

    return value;
}
}  // namespace

void DocumentSourceParquet::appendFirstValueFromColumn(ColumnInfo& column,
                                                       BSONObjBuilder& builder) {
    // The caller should have ensured that there are values left in this column.
    invariant(column.reader->HasNext());
    StringData fieldName = column.descriptor.name();

    // TODO: Handle "converted type" / "logical type"? Right now just handling the primitive types.
    switch (column.descriptor.physical_type()) {
        case parquet::Type::BOOLEAN: {
            bool value = readSingleColumnValue<bool, parquet::BoolReader>(column.reader.get());
            builder.append(fieldName, value);
            return;
        }
        case parquet::Type::INT32: {
            int32_t value =
                readSingleColumnValue<int32_t, parquet::Int32Reader>(column.reader.get());
            builder.append(fieldName, value);
            return;
        }
        case parquet::Type::INT64: {
            int64_t value =
                readSingleColumnValue<int64_t, parquet::Int64Reader>(column.reader.get());
            builder.append(fieldName, value);
            return;
        }
        case parquet::Type::INT96: {
            // TODO: Use BSON BinData for these? For now just omitting them from the output.
            return;
        }
        case parquet::Type::FLOAT: {
            float floatValue =
                readSingleColumnValue<float, parquet::FloatReader>(column.reader.get());
            // Convert the float to a double, since BSON does not have single-precision floating
            // point.
            double doubleValue = floatValue;
            builder.append(fieldName, doubleValue);
            return;
        }
        case parquet::Type::DOUBLE: {
            double value =
                readSingleColumnValue<double, parquet::DoubleReader>(column.reader.get());
            builder.append(fieldName, value);
            return;
        }
        case parquet::Type::BYTE_ARRAY: {
            parquet::ByteArray value =
                readSingleColumnValue<parquet::ByteArray, parquet::ByteArrayReader>(
                    column.reader.get());

            // TODO: Handle additional logical types.
            switch (column.descriptor.logical_type()->type()) {
                case parquet::LogicalType::Type::STRING:
                    builder.append(
                        fieldName, reinterpret_cast<const char*>(value.ptr), value.len + 1);
                    return;
                default:
                    // Default to using BSON "genneral" BinData.
                    builder.appendBinData(
                        fieldName, value.len, BinDataType::BinDataGeneral, value.ptr);
                    return;
            }
        }
        case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
            uasserted(6000001, "not implemented");
        }
        case parquet::Type::UNDEFINED:
            uasserted(6000002, "physical type was UNDEFINED");
    }
}

Document DocumentSourceParquet::convertRow() {
    // TODO: Should we convert directly to Document, or to BSON first?
    BSONObjBuilder rowBuilder;

    for (auto&& column : _columns) {
        appendFirstValueFromColumn(column, rowBuilder);
    }

    // We're done with the current row, so make sure to increment the counter.
    ++_curRow;
    return Document{rowBuilder.obj()};
}

DocumentSource::GetNextResult DocumentSourceParquet::doGetNext() {
    if (_curRow >= _totalRowsInGroup) {
        // We've already returned all of the rows in this group. Initialize state for the next
        // group.
        initForNextRowGroup();
    }

    if (_curRowGroup >= _totalRowGroups) {
        // We've finished pivoting all row groups.
        return GetNextResult::makeEOF();
    }

    // At this point, we know that there is a row for us to return. And the counters for the current
    // row group as well as the current row within the group should reflect this.
    invariant(_curRowGroup >= 0);
    invariant(_curRowGroup < _totalRowGroups);
    invariant(_curRow >= 0);
    invariant(_curRow < _totalRowsInGroup);

    return convertRow();
}

}  // namespace mongo
