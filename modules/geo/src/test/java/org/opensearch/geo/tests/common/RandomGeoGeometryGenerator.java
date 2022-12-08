/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.tests.common;

import org.junit.Assert;
import org.opensearch.geo.algorithm.PolygonGenerator;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.GeometryCollection;
import org.opensearch.geometry.Line;
import org.opensearch.geometry.LinearRing;
import org.opensearch.geometry.MultiLine;
import org.opensearch.geometry.MultiPoint;
import org.opensearch.geometry.MultiPolygon;
import org.opensearch.geometry.Point;
import org.opensearch.geometry.Polygon;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geometry.ShapeType;
import org.opensearch.index.mapper.GeoShapeIndexer;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Random geo generation utilities for randomized geo_shape type testing.
 */
public class RandomGeoGeometryGenerator {
    // Just picking a number 10 to be the max edges of a polygon. Don't want to make too large which can impact
    // debugging.
    private static final int MAX_VERTEXES = 10;
    private static final int MAX_MULTIPLE_GEOMETRIES = 10;

    private static final Predicate<ShapeType> NOT_SUPPORTED_SHAPES = shapeType -> shapeType != ShapeType.CIRCLE
        && shapeType != ShapeType.LINEARRING;

    /**
     * Creating list of only supported geometries defined here: {@link GeoShapeIndexer#prepareForIndexing(Geometry)}
     */
    private static final List<ShapeType> SUPPORTED_SHAPE_TYPES = Arrays.stream(ShapeType.values())
        .filter(NOT_SUPPORTED_SHAPES)
        .collect(Collectors.toList());

    /**
     * Returns a random Geometry. It makes sure that only that geometry is returned which is supported by OpenSearch
     * while indexing. Check {@link GeoShapeIndexer#prepareForIndexing(Geometry)}
     *
     * @return {@link Geometry}
     */
    public static Geometry randomGeometry(final Random r) {
        final ShapeType randomShapeType = SUPPORTED_SHAPE_TYPES.get(
            OpenSearchTestCase.randomIntBetween(0, SUPPORTED_SHAPE_TYPES.size() - 1)
        );
        switch (randomShapeType) {
            case POINT:
                return randomPoint(r);
            case MULTIPOINT:
                return randomMultiPoint(r);
            case POLYGON:
                return randomPolygon(r);
            case LINESTRING:
                return randomLine(r);
            case MULTIPOLYGON:
                return randomMultiPolygon(r);
            case GEOMETRYCOLLECTION:
                return randomGeometryCollection(r);
            case MULTILINESTRING:
                return randomMultiLine(r);
            case ENVELOPE:
                return randomRectangle(r);
            default:
                Assert.fail(String.format(Locale.ROOT, "Cannot create a geometry of type %s ", randomShapeType));
        }
        return null;
    }

    /**
     * Generate a random point on the Earth Surface.
     *
     * @param r {@link Random}
     * @return {@link Point}
     */
    public static Point randomPoint(final Random r) {
        double[] pt = getLonAndLatitude(r);
        return new Point(pt[0], pt[1]);
    }

    /**
     * Generate a random polygon on earth surface.
     *
     * @param r {@link Random}
     * @return {@link Polygon}
     */
    public static Polygon randomPolygon(final Random r) {
        final int vertexCount = OpenSearchTestCase.randomIntBetween(3, MAX_VERTEXES);
        return randomPolygonWithFixedVertexCount(r, vertexCount);
    }

    /**
     * Generate a random line on the earth Surface.
     *
     * @param r {@link Random}
     * @return {@link Line}
     */
    public static Line randomLine(final Random r) {
        final double[] pt1 = getLonAndLatitude(r);
        final double[] pt2 = getLonAndLatitude(r);
        final double[] x = { pt1[0], pt2[0] };
        final double[] y = { pt1[1], pt2[1] };
        return new Line(x, y);
    }

    /**
     * Returns an object of {@link MultiPoint} denoting a list of points on earth surface.
     * @param r {@link Random}
     * @return {@link MultiPoint}
     */
    public static MultiPoint randomMultiPoint(final Random r) {
        int multiplePoints = OpenSearchTestCase.randomIntBetween(1, MAX_MULTIPLE_GEOMETRIES);
        final List<Point> pointsList = new ArrayList<>();
        IntStream.range(0, multiplePoints).forEach(i -> pointsList.add(randomPoint(r)));
        return new MultiPoint(pointsList);
    }

    /**
     * Returns an object of {@link MultiPolygon} denoting various polygons on earth surface.
     *
     * @param r {@link Random}
     * @return {@link MultiPolygon}
     */
    public static MultiPolygon randomMultiPolygon(final Random r) {
        int multiplePolygons = OpenSearchTestCase.randomIntBetween(1, MAX_MULTIPLE_GEOMETRIES);
        final List<Polygon> polygonList = new ArrayList<>();
        IntStream.range(0, multiplePolygons).forEach(i -> polygonList.add(randomPolygon(r)));
        return new MultiPolygon(polygonList);
    }

    /**
     * Returns an object of {@link GeometryCollection} having various shapes on earth surface.
     *
     * @param r {@link Random}
     * @return {@link GeometryCollection}
     */
    public static GeometryCollection<?> randomGeometryCollection(final Random r) {
        final List<Geometry> geometries = new ArrayList<>();
        geometries.addAll(randomMultiPoint(r).getAll());
        geometries.addAll(randomMultiPolygon(r).getAll());
        geometries.addAll(randomMultiLine(r).getAll());
        geometries.add(randomPoint(r));
        geometries.add(randomLine(r));
        geometries.add(randomPolygon(r));
        geometries.add(randomRectangle(r));
        return new GeometryCollection<>(geometries);
    }

    /**
     * Returns a {@link MultiLine} object containing multiple lines on earth surface.
     *
     * @param r {@link Random}
     * @return {@link MultiLine}
     */
    public static MultiLine randomMultiLine(Random r) {
        int multiLines = OpenSearchTestCase.randomIntBetween(1, MAX_MULTIPLE_GEOMETRIES);
        final List<Line> linesList = new ArrayList<>();
        IntStream.range(0, multiLines).forEach(i -> linesList.add(randomLine(r)));
        return new MultiLine(linesList);
    }

    /**
     * Returns a random {@link Rectangle} created on earth surface.
     *
     * @param r {@link Random}
     * @return {@link Rectangle}
     */
    public static Rectangle randomRectangle(final Random r) {
        final Polygon polygon = randomPolygonWithFixedVertexCount(r, 4);
        double minX = Double.POSITIVE_INFINITY, maxX = Double.NEGATIVE_INFINITY, maxY = Double.NEGATIVE_INFINITY, minY =
            Double.POSITIVE_INFINITY;
        for (int i = 0; i < polygon.getPolygon().length(); i++) {
            double x = polygon.getPolygon().getX()[i];
            double y = polygon.getPolygon().getY()[i];

            minX = Math.min(minX, x);
            minY = Math.min(minY, y);
            maxX = Math.max(maxX, x);
            maxY = Math.max(maxY, y);
        }
        return new Rectangle(minX, maxX, maxY, minY);
    }

    public static List<Point> getPointsOfGeometry(final Geometry geometry) {
        final List<Point> pointsList = new ArrayList<>();
        switch (geometry.type()) {
            case POINT:
                pointsList.add((Point) geometry);
                break;
            case MULTIPOINT:
                pointsList.addAll(((MultiPoint) geometry).getAll());
                break;
            case POLYGON:
                final Polygon polygon = (Polygon) geometry;
                final double[] y = polygon.getPolygon().getY();
                final double[] x = polygon.getPolygon().getX();
                for (int i = 0; i < x.length; i++) {
                    pointsList.add(new Point(x[i], y[i]));
                }
                break;
            case LINESTRING:
                final Line line = (Line) geometry;
                final double[] yLine = line.getY();
                final double[] xLine = line.getX();
                for (int i = 0; i < xLine.length; i++) {
                    pointsList.add(new Point(xLine[i], yLine[i]));
                }
                break;
            case MULTIPOLYGON:
                final MultiPolygon multiPolygon = (MultiPolygon) geometry;
                for (int i = 0; i < multiPolygon.size(); i++) {
                    pointsList.addAll(getPointsOfGeometry(multiPolygon.get(i)));
                }
                break;
            case GEOMETRYCOLLECTION:
                final GeometryCollection<Geometry> geometries = (GeometryCollection<Geometry>) geometry;
                geometries.getAll().forEach(geo -> pointsList.addAll(getPointsOfGeometry(geo)));
                break;
            case MULTILINESTRING:
                final MultiLine multiLine = (MultiLine) geometry;
                multiLine.getAll().forEach(l -> pointsList.addAll(getPointsOfGeometry(l)));
                break;
            case ENVELOPE:
                final Rectangle rectangle = (Rectangle) geometry;
                pointsList.add(new Point(rectangle.getMaxX(), rectangle.getMaxY()));
                pointsList.add(new Point(rectangle.getMaxX(), rectangle.getMinY()));
                pointsList.add(new Point(rectangle.getMinX(), rectangle.getMaxY()));
                pointsList.add(new Point(rectangle.getMinX(), rectangle.getMinY()));
                break;
            default:
                Assert.fail(
                    String.format(Locale.ROOT, "Cannot get points of a geometry of type %s, as it is not " + "supported", geometry.type())
                );
        }
        return pointsList;
    }

    /**
     * Returns a double array where pt[0] : longitude and pt[1] : latitude
     *
     * @param r {@link Random}
     * @return double[]
     */
    private static double[] getLonAndLatitude(final Random r) {
        double[] pt = new double[2];
        RandomGeoGenerator.randomPoint(r, pt);
        return pt;
    }

    private static Polygon randomPolygonWithFixedVertexCount(final Random r, final int vertexCount) {
        final List<Double> xPool = new ArrayList<>(vertexCount);
        final List<Double> yPool = new ArrayList<>(vertexCount);
        IntStream.range(0, vertexCount).forEach(iterator -> {
            double[] pt = getLonAndLatitude(r);
            xPool.add(pt[0]);
            yPool.add(pt[1]);
        });
        final List<double[]> pointsList = PolygonGenerator.generatePolygon(xPool, yPool, r);
        // Checking the list
        assert vertexCount == pointsList.get(0).length;
        assert vertexCount == pointsList.get(1).length;
        // Create the linearRing, as we need to close the polygon hence increasing vertexes count by 1
        final double[] x = new double[vertexCount + 1];
        final double[] y = new double[vertexCount + 1];
        IntStream.range(0, vertexCount).forEach(iterator -> {
            x[iterator] = pointsList.get(0)[iterator];
            y[iterator] = pointsList.get(1)[iterator];
        });
        // making sure to close the polygon
        x[vertexCount] = x[0];
        y[vertexCount] = y[0];
        final LinearRing linearRing = new LinearRing(x, y);
        return new Polygon(linearRing);
    }

}
