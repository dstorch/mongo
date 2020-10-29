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

#pragma once

#include <parquet/api/reader.h>

#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/pipeline/document_source.h"

namespace mongo {

/**
 * A DocumentSource that reads a local parquet file from disk and converts it into BSON for
 * consumption by downstream stages. This involves pivoting the column-oriented storage in the
 * parquet file into a row-oriented BSON format.
 */
class DocumentSourceParquet : public DocumentSource {
public:
    static constexpr StringData kStageName = "parquet"_sd;

    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx);

    DocumentSourceParquet(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                          std::string fileName);
    virtual ~DocumentSourceParquet() = default;

    const char* getSourceName() const override;

    Value serialize(
        boost::optional<ExplainOptions::Verbosity> explain = boost::none) const override {
        return Value{Document{{getSourceName(), _fileName}}};
    }

    StageConstraints constraints(Pipeline::SplitState pipeState) const override {
        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kFirst,
                                     HostTypeRequirement::kNone,
                                     DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kNotAllowed,
                                     TransactionRequirement::kAllowed,
                                     LookupRequirement::kAllowed,
                                     UnionRequirement::kAllowed);

        constraints.requiresInputDocSource = false;
        return constraints;
    }

    /**
     * TODO
     */
    GetModPathsReturn getModifiedPaths() const override {
        return {GetModPathsReturn::Type::kFiniteSet, std::set<std::string>{}, {}};
    }

    boost::optional<DistributedPlanLogic> distributedPlanLogic() override {
        return boost::none;
    }

protected:
    /**
     * Pairs a reader for a single column in a parquet file with a description of that column's
     * metadata.
     */
    struct ColumnInfo {
        ColumnInfo(std::shared_ptr<parquet::ColumnReader> reader,
                   const parquet::ColumnDescriptor& descriptor)
            : reader(std::move(reader)), descriptor(descriptor) {}

        std::shared_ptr<parquet::ColumnReader> reader;
        const parquet::ColumnDescriptor& descriptor;
    };

    GetNextResult doGetNext() override;

    /**
     * TODO
     */
    void initForNextRowGroup();

    /**
     * TODO
     */
    Document convertRow();

    /**
     * TODO
     */
    void appendFirstValueFromColumn(ColumnInfo& column, BSONObjBuilder& builder);

    // File path for the input parquet file.
    const std::string _fileName;

    // Top-level reader for the parquet file, provided by arrow library.
    const std::shared_ptr<parquet::ParquetFileReader> _fileReader;

    // The total number of row groups in the input file.
    const int _totalRowGroups;

    // Parquet files are split into row groups. Keep track of which one we are on.
    int _curRowGroup = -1;

    // The number of rows converted so far in the current row group, as well as the total number of
    // rows in the row group.
    int _totalRowsInGroup = -1;
    int _curRow = -1;

    // A reader and descritptor for each column in the parquet file.
    std::vector<ColumnInfo> _columns;
};

}  // namespace mongo
