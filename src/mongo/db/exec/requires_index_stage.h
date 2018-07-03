/**
 * Copyright (C) 2018 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#pragma once

#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/catalog/index_catalog_entry.h"
#include "mongo/db/exec/requires_collection_stage.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/index/index_descriptor.h"

namespace mongo {

/**
 * TODO: Write comment.
 */
class RequiresIndexStage : public RequiresCollectionStage {
public:
    RequiresIndexStage(const char* stageType,
                       OperationContext* opCtx,
                       const IndexDescriptor* indexDescriptor)
        : RequiresCollectionStage(stageType, opCtx, indexDescriptor->getCollection()),
          _indexDescriptor(indexDescriptor),
          _indexCatalogEntry(collection()->getIndexCatalog()->getEntry(indexDescriptor)),
          _indexAccessMethod(_indexCatalogEntry->accessMethod()),
          _indexName(indexDescriptor->indexName()),
          _generationCount(_indexCatalogEntry->generationCount()) {}

    virtual ~RequiresIndexStage() = default;

protected:
    void doRequiresCollectionStageSaveState() override final;

    void doRequiresCollectionStageRestoreState() override final;

    /**
     * TODO: Comment.
     * TODO: This is a terrible name.
     */
    virtual void doRequiresIndexStageSaveState() = 0;

    /**
     * TODO: Comment.
     * TODO: This is a terrible name.
     */
    virtual void doRequiresIndexStageRestoreState() = 0;

protected:
    const IndexAccessMethod* getIndexAccessMethod() const {
        return _indexAccessMethod;
    }

    const IndexDescriptor* getIndexDescriptor() const {
        return _indexDescriptor;
    }

private:
    const IndexDescriptor* _indexDescriptor;
    const IndexCatalogEntry* _indexCatalogEntry;
    const IndexAccessMethod* _indexAccessMethod;

    const std::string _indexName;
    const unsigned long long _generationCount;
};

}  // namespace mongo
