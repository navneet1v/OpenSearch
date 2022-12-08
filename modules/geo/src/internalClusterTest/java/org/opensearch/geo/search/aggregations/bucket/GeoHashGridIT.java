/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.geo.search.aggregations.bucket;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.cursors.ObjectIntCursor;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.index.PointValues;
import org.junit.Assert;
import org.opensearch.Version;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.geo.GeoJson;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoShapeDocValue;
import org.opensearch.common.geo.GeoShapeUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.geo.GeoModulePluginIntegTestCase;
import org.opensearch.geo.search.aggregations.bucket.geogrid.GeoGrid;
import org.opensearch.geo.search.aggregations.common.GeoBoundsHelper;
import org.opensearch.geo.tests.common.AggregationBuilders;
import org.opensearch.geo.tests.common.RandomGeoGeometryGenerator;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.Line;
import org.opensearch.geometry.LinearRing;
import org.opensearch.geometry.MultiLine;
import org.opensearch.geometry.Point;
import org.opensearch.geometry.Polygon;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geometry.utils.Geohash;
import org.opensearch.geometry.utils.StandardValidator;
import org.opensearch.geometry.utils.WellKnownText;
import org.opensearch.index.query.GeoBoundingBoxQueryBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.geometry.utils.Geohash.PRECISION;
import static org.opensearch.geometry.utils.Geohash.stringEncode;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class GeoHashGridIT extends GeoModulePluginIntegTestCase {

    private static final int MAX_PRECISION = 4;

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    private Version version = VersionUtils.randomIndexCompatibleVersion(random());
    private static final WellKnownText WKT = new WellKnownText(true, new StandardValidator(true));
    private static ObjectIntMap<String> expectedDocCountsForGeohash;
    private static ObjectIntMap<String> multiValuedExpectedDocCountsForGeohash;

    private static ObjectIntMap<String> expectedDocCountsForGeoshapeGeohash;

    private static final int NUM_DOCS = 100;

    private static String SMALLEST_GEO_HASH = null;
    private static final String GEO_SHAPE_INDEX_NAME = "geoshape_index";

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        Random random = random();
        prepareSingleValueGeoPointIndex(random);
        prepareMultiValuedGeoPointIndex(random);
        prepareGeoShapeIndex(random);
        ensureSearchable();
    }

    public void testSimple() throws Exception {
        for (int precision = 1; precision <= PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("idx")
                .addAggregation(AggregationBuilders.geohashGrid("geohashgrid").field("location").precision(precision))
                .get();

            assertSearchResponse(response);

            GeoGrid geoGrid = response.getAggregations().get("geohashgrid");
            List<? extends GeoGrid.Bucket> buckets = geoGrid.getBuckets();
            Object[] propertiesKeys = (Object[]) ((InternalAggregation) geoGrid).getProperty("_key");
            Object[] propertiesDocCounts = (Object[]) ((InternalAggregation) geoGrid).getProperty("_count");
            for (int i = 0; i < buckets.size(); i++) {
                GeoGrid.Bucket cell = buckets.get(i);
                String geohash = cell.getKeyAsString();

                long bucketCount = cell.getDocCount();
                int expectedBucketCount = expectedDocCountsForGeohash.get(geohash);
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);
                GeoPoint geoPoint = (GeoPoint) propertiesKeys[i];
                assertThat(stringEncode(geoPoint.lon(), geoPoint.lat(), precision), equalTo(geohash));
                assertThat((long) propertiesDocCounts[i], equalTo(bucketCount));
            }
        }
    }

    public void testSimpleGeoShapes() {
        for (int precision = 4; precision <= MAX_PRECISION; precision++) {
            SearchResponse response = client().prepareSearch(GEO_SHAPE_INDEX_NAME)
                .addAggregation(AggregationBuilders.geohashGrid("geohashgrid").field("location").precision(precision))
                .get();

            SearchResponse res = client().prepareSearch(GEO_SHAPE_INDEX_NAME).get();
            System.out.println("*************** Search Response is : " + res.toString() + " \n \n " +
                    "*******************");
            assertSearchResponse(response);
//            System.out.println("*************** Search Response is : " + response.toString() + " \n \n " +
//                    "*******************");
            GeoGrid geoGrid = response.getAggregations().get("geohashgrid");
            List<? extends GeoGrid.Bucket> buckets = geoGrid.getBuckets();
            Object[] propertiesKeys = (Object[]) ((InternalAggregation) geoGrid).getProperty("_key");
            Object[] propertiesDocCounts = (Object[]) ((InternalAggregation) geoGrid).getProperty("_count");
            System.out.println("Precision : " + precision);
            for (int i = 0; i < buckets.size(); i++) {
                GeoGrid.Bucket cell = buckets.get(i);
                String geohash = cell.getKeyAsString();

                long bucketCount = cell.getDocCount();
                int expectedBucketCount = expectedDocCountsForGeoshapeGeohash.get(geohash);
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);
                GeoPoint geoPoint = (GeoPoint) propertiesKeys[i];
                assertThat(stringEncode(geoPoint.lon(), geoPoint.lat(), precision), equalTo(geohash));
                assertThat((long) propertiesDocCounts[i], equalTo(bucketCount));
            }
        }
    }

    public void testMultivalued() throws Exception {
        for (int precision = 1; precision <= PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("multi_valued_idx")
                .addAggregation(AggregationBuilders.geohashGrid("geohashgrid").field("location").precision(precision))
                .get();

            assertSearchResponse(response);

            GeoGrid geoGrid = response.getAggregations().get("geohashgrid");
            for (GeoGrid.Bucket cell : geoGrid.getBuckets()) {
                String geohash = cell.getKeyAsString();

                long bucketCount = cell.getDocCount();
                int expectedBucketCount = multiValuedExpectedDocCountsForGeohash.get(geohash);
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);
            }
        }
    }

    public void testFiltered() throws Exception {
        GeoBoundingBoxQueryBuilder bbox = new GeoBoundingBoxQueryBuilder("location");
        bbox.setCorners(SMALLEST_GEO_HASH).queryName("bbox");
        for (int precision = 1; precision <= PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("idx")
                .addAggregation(
                    org.opensearch.search.aggregations.AggregationBuilders.filter("filtered", bbox)
                        .subAggregation(AggregationBuilders.geohashGrid("geohashgrid").field("location").precision(precision))
                )
                .get();

            assertSearchResponse(response);

            Filter filter = response.getAggregations().get("filtered");

            GeoGrid geoGrid = filter.getAggregations().get("geohashgrid");
            for (GeoGrid.Bucket cell : geoGrid.getBuckets()) {
                String geohash = cell.getKeyAsString();
                long bucketCount = cell.getDocCount();
                int expectedBucketCount = expectedDocCountsForGeohash.get(geohash);
                assertNotSame(bucketCount, 0);
                assertTrue("Buckets must be filtered", geohash.startsWith(SMALLEST_GEO_HASH));
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);

            }
        }
    }

    public void testUnmapped() throws Exception {
        for (int precision = 1; precision <= PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(AggregationBuilders.geohashGrid("geohashgrid").field("location").precision(precision))
                .get();

            assertSearchResponse(response);

            GeoGrid geoGrid = response.getAggregations().get("geohashgrid");
            assertThat(geoGrid.getBuckets().size(), equalTo(0));
        }

    }

    public void testPartiallyUnmapped() throws Exception {
        for (int precision = 1; precision <= PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(AggregationBuilders.geohashGrid("geohashgrid").field("location").precision(precision))
                .get();

            assertSearchResponse(response);

            GeoGrid geoGrid = response.getAggregations().get("geohashgrid");
            for (GeoGrid.Bucket cell : geoGrid.getBuckets()) {
                String geohash = cell.getKeyAsString();

                long bucketCount = cell.getDocCount();
                int expectedBucketCount = expectedDocCountsForGeohash.get(geohash);
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);
            }
        }
    }

    public void testTopMatch() throws Exception {
        for (int precision = 1; precision <= PRECISION; precision++) {
            SearchResponse response = client().prepareSearch("idx")
                .addAggregation(
                    AggregationBuilders.geohashGrid("geohashgrid").field("location").size(1).shardSize(100).precision(precision)
                )
                .get();

            assertSearchResponse(response);

            GeoGrid geoGrid = response.getAggregations().get("geohashgrid");
            // Check we only have one bucket with the best match for that resolution
            assertThat(geoGrid.getBuckets().size(), equalTo(1));
            for (GeoGrid.Bucket cell : geoGrid.getBuckets()) {
                String geohash = cell.getKeyAsString();
                long bucketCount = cell.getDocCount();
                int expectedBucketCount = 0;
                for (ObjectIntCursor<String> cursor : expectedDocCountsForGeohash) {
                    if (cursor.key.length() == precision) {
                        expectedBucketCount = Math.max(expectedBucketCount, cursor.value);
                    }
                }
                assertNotSame(bucketCount, 0);
                assertEquals("Geohash " + geohash + " has wrong doc count ", expectedBucketCount, bucketCount);
            }
        }
    }

    public void testSizeIsZero() {
        final int size = 0;
        final int shardSize = 10000;
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareSearch("idx")
                .addAggregation(AggregationBuilders.geohashGrid("geohashgrid").field("location").size(size).shardSize(shardSize))
                .get()
        );
        assertThat(exception.getMessage(), containsString("[size] must be greater than 0. Found [0] in [geohashgrid]"));
    }

    public void testShardSizeIsZero() {
        final int size = 100;
        final int shardSize = 0;
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareSearch("idx")
                .addAggregation(AggregationBuilders.geohashGrid("geohashgrid").field("location").size(size).shardSize(shardSize))
                .get()
        );
        assertThat(exception.getMessage(), containsString("[shardSize] must be greater than 0. Found [0] in [geohashgrid]"));
    }

    private void prepareSingleValueGeoPointIndex(final Random random) throws Exception {
        createIndex("idx_unmapped");
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        assertAcked(prepareCreate("idx").setSettings(settings).setMapping("location", "type=geo_point", "city", "type=keyword"));
        final List<IndexRequestBuilder> cities = new ArrayList<>();
        expectedDocCountsForGeohash = new ObjectIntHashMap<>(NUM_DOCS * 2);
        for (int i = 0; i < NUM_DOCS; i++) {
            // generate random point
            double lat = (180d * random.nextDouble()) - 90d;
            double lng = (360d * random.nextDouble()) - 180d;
            String randomGeoHash = stringEncode(lng, lat, PRECISION);
            // Index at the highest resolution
            cities.add(indexCity("idx", randomGeoHash, lat + ", " + lng));
            expectedDocCountsForGeohash.put(randomGeoHash, expectedDocCountsForGeohash.getOrDefault(randomGeoHash, 0) + 1);
            // Update expected doc counts for all resolutions..
            for (int precision = PRECISION - 1; precision > 0; precision--) {
                String hash = stringEncode(lng, lat, precision);
                if ((SMALLEST_GEO_HASH == null) || (hash.length() < SMALLEST_GEO_HASH.length())) {
                    SMALLEST_GEO_HASH = hash;
                }
                expectedDocCountsForGeohash.put(hash, expectedDocCountsForGeohash.getOrDefault(hash, 0) + 1);
            }
        }
        indexRandom(true, cities);
    }

    private void prepareMultiValuedGeoPointIndex(final Random random) throws Exception {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        final List<IndexRequestBuilder> cities = new ArrayList<>();
        assertAcked(
            prepareCreate("multi_valued_idx").setSettings(settings).setMapping("location", "type=geo_point", "city", "type=keyword")
        );
        multiValuedExpectedDocCountsForGeohash = new ObjectIntHashMap<>(NUM_DOCS * 2);
        for (int i = 0; i < NUM_DOCS; i++) {
            final int numPoints = random.nextInt(4);
            final List<String> points = new ArrayList<>();
            final Set<String> geoHashes = new HashSet<>();
            for (int j = 0; j < numPoints; ++j) {
                final double lat = (180d * random.nextDouble()) - 90d;
                final double lng = (360d * random.nextDouble()) - 180d;
                points.add(lat + "," + lng);
                // Update expected doc counts for all resolutions..
                for (int precision = PRECISION; precision > 0; precision--) {
                    final String geoHash = stringEncode(lng, lat, precision);
                    geoHashes.add(geoHash);
                }
            }
            cities.add(indexCity("multi_valued_idx", Integer.toString(i), points));
            for (final String hash : geoHashes) {
                multiValuedExpectedDocCountsForGeohash.put(hash, multiValuedExpectedDocCountsForGeohash.getOrDefault(hash, 0) + 1);
            }
        }
        indexRandom(true, cities);
    }

    private void prepareGeoShapeIndex(final Random random) throws Exception {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        final List<IndexRequestBuilder> geoshapes = new ArrayList<>();
        assertAcked(prepareCreate(GEO_SHAPE_INDEX_NAME).setSettings(settings).setMapping("location", "type=geo_shape"));
        expectedDocCountsForGeoshapeGeohash = new ObjectIntHashMap<>();
        for (int i = 0; i < 4; i++) {
            final Geometry geometry = RandomGeoGeometryGenerator.randomGeometry(random);
//            final Geometry geometry = WKT.fromWKT("LINESTRING (119.32172608525889 -10.383571469569318, -17" +
//                    ".20785938981669 37" +
//                    ".325362643713305)");
//            final Geometry geometry = WKT.fromWKT("MULTIPOLYGON (((35.339667709312565 74.47005221511768, -86" +
//                    ".67520912557694 37.386143711554006," +
//                    " 29.83009269784222 -33.207985081692584, 76.71041242711354 -59.74241818490327, 142.4129692358762 -82.33844242792424, 146.93863749035938 -53.007678236521016, 159.67416706563824 35.703684738733656, 60.1716913401531 66.87073550954219, 35.339667709312565 74.47005221511768)))");
            System.out.println("Geometry is : " + WKT.toWKT(geometry));

            final GeoPoint topLeft = new GeoPoint();
            final GeoPoint bottomRight = new GeoPoint();
            final GeoShapeDocValue geometryDocValue = GeoShapeDocValue.createGeometryDocValue(geometry);

            //assert geometry != null;
            GeoBoundsHelper.updateBoundsForGeometry(geometry, topLeft, bottomRight);
            System.out.println("Bounds are: " + topLeft + " " + bottomRight);
            final Set<String> geoHashes = new HashSet<>();
            for (int precision = MAX_PRECISION; precision > 0; precision--) {
                final GeoPoint topRight = new GeoPoint(topLeft.getLat(), bottomRight.getLon());
                final GeoPoint bottomLeft = new GeoPoint(bottomRight.getLat(), topLeft.getLon());

                // lon , lat, level
                String currentGeoHash = Geohash.stringEncode(topLeft.getLon(), topLeft.getLat(), precision);
                String startingRowGeoHash = currentGeoHash;
                String endGeoHashForCurrentRow = Geohash.stringEncode(topRight.getLon(), topRight.getLat(), precision);
                String terminatingGeoHash = Geohash.stringEncode(bottomRight.getLon(), bottomRight.getLat(), precision);
                System.out.println("Terminating Geohash: " + terminatingGeoHash + "  currentGeoHash : " + currentGeoHash + " endGeoHashForCurrentRow : " + endGeoHashForCurrentRow);
                System.out.println("South GeoHash: " + Geohash.getNeighbor(startingRowGeoHash, precision, 0, -1));
                System.out.println("Right GeoHash: " + Geohash.getNeighbor(startingRowGeoHash, precision, 1, 0));
                while(true) {
                    Rectangle currentRectangle = Geohash.toBoundingBox(currentGeoHash);

                    if (geometryDocValue.isIntersectingRectangle(currentRectangle)) {
                        geoHashes.add(currentGeoHash);
                    }

                    assert currentGeoHash != null;
                    if(currentGeoHash.equals(terminatingGeoHash)) {
                        break;
                    }
                    if(currentGeoHash.equals(endGeoHashForCurrentRow)) {
                        // go in south direction
                        currentGeoHash = Geohash.getNeighbor(startingRowGeoHash, precision, 0, -1);
                        startingRowGeoHash = currentGeoHash;
                        endGeoHashForCurrentRow = Geohash.getNeighbor(endGeoHashForCurrentRow, precision, 0, -1);
                    } else {
                        currentGeoHash = Geohash.getNeighbor(currentGeoHash, precision, 1, 0);
                    }
                }
            }

            geoshapes.add(indexGeoShape(GEO_SHAPE_INDEX_NAME, geometry));
            for (final String hash : geoHashes) {
                expectedDocCountsForGeoshapeGeohash.put(hash, expectedDocCountsForGeoshapeGeohash.getOrDefault(hash, 0) + 1);
            }
            if(geoHashes.contains("tbp8")) {
                System.out.println(expectedDocCountsForGeoshapeGeohash.get("xphh"));
                System.out.println("Contributing Geometry : " + WKT.toWKT(geometry));
            }
        }

        indexRandom(true, false, geoshapes);
        flushAndRefresh(GEO_SHAPE_INDEX_NAME);
    }

    private IndexRequestBuilder indexGeoShape(final String index, final Geometry geometry) throws Exception {
        XContentBuilder source = jsonBuilder().startObject();
        source = source.field("location", WKT.toWKT(geometry));
        source = source.endObject();
        return client().prepareIndex(index).setSource(source);
    }

    private IndexRequestBuilder indexCity(final String index, final String name, final List<String> latLon) throws Exception {
        XContentBuilder source = jsonBuilder().startObject().field("city", name);
        if (latLon != null) {
            source = source.field("location", latLon);
        }
        source = source.endObject();
        return client().prepareIndex(index).setSource(source);
    }

    private IndexRequestBuilder indexCity(final String index, final String name, final String latLon) throws Exception {
        return indexCity(index, name, List.of(latLon));
    }

    public void testSample() throws Exception {
        Geometry myline = WKT.fromWKT("LINESTRING (130.55761065311816 52.23960723311731, 167.88076729151243 7.540744932329019)");
        final PointValues.Relation myRelation =
                GeoShapeDocValue.createGeometryDocValue(myline).shapeDocValues.relate(LatLonGeometry.create(GeoShapeUtils.toLuceneRectangle(Geohash.toBoundingBox("xphh"))));
        System.out.println("myline " + WKT.toWKT(myline) + "with rectangle relation: " + myRelation);

        Geometry multiLine = WKT.fromWKT("MULTILINESTRING ((39.40148101166176 83.86610699120837, 173.9680519915584 44" +
                ".35699356322226),(-114.39056729322027 47.6270990104083, 32.66199437197801 -13.049474016402385),(130.55761065311816 52.23960723311731, 167.88076729151243 7.540744932329019),(6.862645808409695 -15.576254963995936, -147.4381127836097 48.816234498141455),(-93.6303710326476 -86.68145063317081, 148.32345339297677 -62.50360889122078),(119.32172608525889 -10.383571469569318, -17.20785938981669 37.325362643713305),(3.8226155196961145 -11.09540541570459, 118.90197196578345 -24.00909397150781))");

        GeoShapeDocValue docValue = GeoShapeDocValue.createGeometryDocValue(multiLine);

        Rectangle rectangle = Geohash.toBoundingBox("xphh");
        System.out.println(docValue.isIntersectingRectangle(rectangle));

        final org.apache.lucene.geo.Rectangle luceneRectangle = GeoShapeUtils.toLuceneRectangle(rectangle);
        for(Line line : ((MultiLine)multiLine).getAll()) {
            try {
                final PointValues.Relation relation = GeoShapeDocValue.createGeometryDocValue(line).shapeDocValues.relate(LatLonGeometry.create(luceneRectangle));
                System.out.println("Line " + WKT.toWKT(line) + "with rectangle relation: " + relation);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        GeoShapeDocValue rectangleDocValue = GeoShapeDocValue.createGeometryDocValue(rectangle);

        MultiLine multiLine1 = (MultiLine) multiLine;

        for(Line line : multiLine1.getAll()) {
            final org.apache.lucene.geo.Line luceneLine = GeoShapeUtils.toLuceneLine(line);
            try {
                final PointValues.Relation relation =
                        rectangleDocValue.shapeDocValues.relate(LatLonGeometry.create(luceneLine));
                System.out.println("Line : " + WKT.toWKT(line) + " is " + relation);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

//        Rectangle rectangle = (Rectangle) WKT.fromWKT("BBOX (-74.09969553413072, 171.9012071196218, 65.21191961514646, -31" +
//                ".076016084089076)");
        double[] x = new double[5];
        double[] y = new double[5];
        // Lat : y, lon: x
        x[0] = rectangle.getMaxX();
        y[0] = rectangle.getMaxY();

        x[1] = rectangle.getMaxX();
        y[1] = rectangle.getMinY();

        x[2] = rectangle.getMinX();
        y[2] = rectangle.getMinY();

        x[3] = rectangle.getMinX();
        y[3] = rectangle.getMaxY();

        x[4] = rectangle.getMaxX();
        y[4] = rectangle.getMaxY();

        LinearRing linearRing = new LinearRing(x, y);
        Polygon polygon = new Polygon(linearRing, Collections.emptyList());
        System.out.println(" " + WKT.toWKT(polygon));

        Assert.assertTrue(true);
    }

}
