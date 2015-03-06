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

#pragma once

#include <memory>

#include "mongo/base/owned_pointer_map.h"
#include "mongo/db/query/canonical_query.h"
#include "mongo/db/query/shapeanalyzer/query_log_parser.h"

namespace mongo {

    struct ShapeAnalysisKey {
        PlanCacheKey cacheKey;
        std::string ns;
    };

    struct ShapeAnalysisKeyHasher {
        size_t operator()(const ShapeAnalysisKey& key) const {
            return ((std::hash<std::string>()(key.cacheKey) ^
                    (std::hash<std::string>()(key.ns) << 1)) >> 1);
        }
    };

    struct ShapeAnalysisKeyComparator {
        bool operator()(const ShapeAnalysisKey& left, const ShapeAnalysisKey& right) const {
            return (left.cacheKey == right.cacheKey) && (left.ns == right.ns);
        }
    };

    struct ShapeAnalysisResult {

        ShapeAnalysisResult();

        void computeStats();

        void report() const;

        size_t timesSeen;

        std::string ns;

        // We just store these for the first instance of the shape.
        BSONObj predicate;
        BSONObj projection;
        BSONObj sort;

        // We store these each time we see the shape.
        std::vector<size_t> millis;

        // These get filled out during the stats computation phase.
        double meanMillis;
        size_t minMillis;
        size_t maxMillis;
    };

    class ShapeAnalyzer {
    public:
        void add(const QueryLogParser& logParser);

        void computeStats();

        void report() const;

    private:
        std::unordered_map<ShapeAnalysisKey,
                           ShapeAnalysisResult,
                           ShapeAnalysisKeyHasher,
                           ShapeAnalysisKeyComparator> _shapes;

        std::vector<ShapeAnalysisResult> _sortedShapes;
    };

} // namespace mongo
