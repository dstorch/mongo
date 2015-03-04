/*    Copyright 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#include "mongo/platform/basic.h"

#include <memory>

#include "mongo/db/json.h"
#include "mongo/db/query/shapeanalyzer/shapeanalyzer.h"

namespace mongo {

namespace {

    Status fromjsonSafe(const std::string& json, BSONObj* out) {
        *out = fromjson(json);
        return Status::OK();
    }

} // namespace

    void ShapeAnalysisResult::log(std::ostream& out) const {
        out << ns << "\t"
            << rawPredicate << "\t"
            << rawProjection << "\t"
            << rawSort << "\t"
            << cacheKey << "\t"
            << std::endl;
    }

    std::string ShapeAnalyzer::empty = "{}";

    StatusWith<ShapeAnalysisResult> ShapeAnalyzer::analyze(const std::string& ns,
                                                           const std::string& predicate) {
        return analyze(ns, predicate, ShapeAnalyzer::empty, ShapeAnalyzer::empty);
    }

    StatusWith<ShapeAnalysisResult> ShapeAnalyzer::analyze(const std::string& ns,
                                                           const std::string& predicate,
                                                           const std::string& projection,
                                                           const std::string& sort) {
        BSONObj predicateObj, projectionObj, sortObj;

        Status predicateStatus = fromjsonSafe(predicate, &predicateObj);
        if (!predicateStatus.isOK()) {
            return StatusWith<ShapeAnalysisResult>(predicateStatus);
        }

        Status projectionStatus = fromjsonSafe(projection, &projectionObj);
        if (!projectionStatus.isOK()) {
            return StatusWith<ShapeAnalysisResult>(projectionStatus);
        }

        Status sortStatus = fromjsonSafe(sort, &sortObj);
        if (!sortStatus.isOK()) {
            return StatusWith<ShapeAnalysisResult>(sortStatus);
        }

        std::unique_ptr<CanonicalQuery> cq;
        {
            CanonicalQuery* cqRaw;
            Status status = CanonicalQuery::canonicalize(ns,
                                                         predicateObj,
                                                         sortObj,
                                                         projectionObj,
                                                         &cqRaw);
            if (!status.isOK()) {
                return StatusWith<ShapeAnalysisResult>(status);
            }

            cq.reset(cqRaw);
        }

        return analyze(std::move(cq));
    }

    StatusWith<ShapeAnalysisResult> ShapeAnalyzer::analyze(std::unique_ptr<CanonicalQuery> cq) {
        ShapeAnalysisResult result;

        result.ns = cq->ns();

        result.rawPredicate = cq->getParsed().getFilter();
        result.rawProjection = cq->getParsed().getProj();
        result.rawSort = cq->getParsed().getSort();

        const PlanCacheKey& key = cq->getPlanCacheKey();
        result.cacheKey = key;

        return StatusWith<ShapeAnalysisResult>(result);
    }

} // namespace mongo
