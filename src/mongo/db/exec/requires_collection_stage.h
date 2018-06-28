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

#include "mongo/db/catalog/collection.h"
#include "mongo/db/exec/plan_stage.h"
#include "mongo/util/uuid.h"

namespace mongo {

/**
 * TODO: Write comment.
 */
class RequiresCollectionStage : public PlanStage {
public:
    // TODO: I think this should maybe be Collection* instead of const Collection*.
    RequiresCollectionStage(const char* stageType, OperationContext* opCtx, const Collection* coll)
        : PlanStage(stageType, opCtx),
          _collection(coll),
          // TODO: This assumes that the UUID is non-none. Is that a safe assumption?
          _collectionUUID(_collection->uuid().get()) {}

    virtual ~RequiresCollectionStage() = default;

protected:
    void doSaveState() final;

    void doRestoreState() final;

    /**
     * TODO: Comment.
     * TODO: This is a terrible name.
     */
    virtual void doRequiresCollectionStageSaveState() = 0;

    /**
     * TODO: Comment.
     * TODO: This is a terrible name.
     */
    virtual void doRequiresCollectionStageRestoreState() = 0;

    const Collection* collection() {
        return _collection;
    }

    UUID uuid() {
        return _collectionUUID;
    }

private:
    const Collection* _collection;
    UUID _collectionUUID;
};

}  // namespace mongo
