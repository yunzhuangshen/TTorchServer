package au.edu.rmit.trajectory.similarity.query;

import au.edu.rmit.trajectory.similarity.model.MMEdge;
import au.edu.rmit.trajectory.similarity.model.MMPoint;
import au.edu.rmit.trajectory.similarity.util.GeoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author forrest0402
 * @Description
 * @date 12/4/2017
 */
public class ShortestPathQuery implements Serializable {

    public Map<Integer, ShortestPathEntry> entryMap = new HashMap<>();

    private double minDisBetweenTowerPoints(MMPoint p1, MMPoint p2) {
        if (p1 == p2) return 0.0;
        Double res = entryMap.get(p1.hashCode()).dist.get(p2.hashCode());
        if (res == null) return Double.MAX_VALUE;
        return res.doubleValue();
    }

    /**
     * Get minimum distance from p1 to p2, p1 must be a tower candidatePoint
     * if p2 is tower candidatePoint, the answer will be retrieved constantly
     * otherwise, minDistance will first calculate the distance from p1 to the nearest tower candidatePoint of p2,
     * then add the distance from that tower candidatePoint to p2.
     *
     * @param p1
     * @param p2
     * @return
     */
    private double minDisBetweenTowerPointAndPillarPoint(MMPoint p1, MMPoint p2) {
        double minDis = Double.MAX_VALUE;
        if (p2.edge.isForward) {
            Double res = entryMap.get(p1.hashCode()).dist.get(p2.edge.basePoint.hashCode());
            if (res != null) {
                minDis = res + p2.toBasePointDistance;
            }
        }
        if (p2.edge.isBackward) {
            Double res = entryMap.get(p1.hashCode()).dist.get(p2.edge.adjPoint.hashCode());
            if (res != null) {
                minDis = Math.min(minDis, res + p2.edge.getLength() - p2.toBasePointDistance);
            }
        }
        return minDis;
    }

    /**
     * p1 must be the pillar candidatePoint and p2 is a tower candidatePoint
     *
     * @param p1
     * @param p2
     * @return
     */
    private double minDisBetweenPillarPointAndTowerPoint(MMPoint p1, MMPoint p2) {
        double minDis = Double.MAX_VALUE;
        if (p1.edge.isBackward) {
            Double res = entryMap.get(p1.edge.basePoint.hashCode()).dist.get(p2.hashCode());
            if (res != null) {
                minDis = res + p1.toBasePointDistance;
            }
        }
        if (p1.edge.isForward) {
            Double res = entryMap.get(p1.edge.adjPoint.hashCode()).dist.get(p2.hashCode());
            if (res != null) {
                minDis = Math.min(minDis, res + p1.edge.getLength() - p1.toBasePointDistance);
            }
        }
        return minDis;
    }

    /**
     * p1 and p2 are both pillar points
     *
     * @param p1
     * @param p2
     * @return
     */
    private double minDisBetweenPillarPoints(MMPoint p1, MMPoint p2) {
        if (p1 == p2) return 0.0;
        if (p1.edge == p2.edge) {
            int flag = 0;
            for (MMPoint point : p1.edge.getPillarPoints()) {
                if (flag != 0) break;
                if (point == p1) flag = 1;
                if (point == p2) flag = 2;
            }
            if ((flag == 1 && p1.edge.isForward) || (flag == 2 && p1.edge.isBackward)) {
                return GeoUtil.distance(p1, p2);
            } else if (flag == 1 && p1.edge.isBackward) {
                //p1->p1.base->p1.adj->p2
                Double dist = entryMap.get(p1.edge.basePoint.hashCode()).dist.get(p1.edge.adjPoint.hashCode());
                if (dist != null)
                    return p1.toBasePointDistance + dist + p2.edge.getLength() - p2.toBasePointDistance;
                else {
                    return Double.MAX_VALUE;
                }
            } else if (flag == 2 && p1.edge.isForward) {
                //p1->p1.adj->p1.base->p2
                Double dist = entryMap.get(p1.edge.adjPoint.hashCode()).dist.get(p1.edge.basePoint.hashCode());
                if (dist != null)
                    return p1.edge.getLength() - p1.toBasePointDistance + dist + p2.toBasePointDistance;
                else {
                    return Double.MAX_VALUE;
                }
            }
        }
        double minDis = Double.MAX_VALUE;
        if (p1.edge.isForward) {
            //p1->p1.adj->p2.base->p2
            if (p2.edge.isForward) {
                Double res = entryMap.get(p1.edge.adjPoint.hashCode()).dist.get(p2.edge.basePoint.hashCode());
                if (res != null) {
                    minDis = res + p1.edge.getLength() - p1.toBasePointDistance + p2.toBasePointDistance;
                }
            }
            //p1->p1.adj->p2.adj->p2
            if (p2.edge.isBackward) {
                Double res = entryMap.get(p1.edge.adjPoint.hashCode()).dist.get(p2.edge.adjPoint.hashCode());
                if (res != null) {
                    minDis = Math.min(minDis, res + p1.edge.getLength() - p1.toBasePointDistance + p2.edge.getLength() - p2.toBasePointDistance);
                }
            }
        }
        if (p1.edge.isBackward) {
            //p1->p1.base->p2.base->p2
            if (p2.edge.isForward) {
                Double res = entryMap.get(p1.edge.basePoint.hashCode()).dist.get(p2.edge.basePoint.hashCode());
                if (res != null) {
                    minDis = Math.min(minDis, res + p1.toBasePointDistance + p2.toBasePointDistance);
                }
            }
            //p1->p1.base->p2.adj->p2
            if (p2.edge.isBackward) {
                Double res = entryMap.get(p1.edge.basePoint.hashCode()).dist.get(p2.edge.adjPoint.hashCode());
                if (res != null) {
                    minDis = Math.min(minDis, res + p1.toBasePointDistance + p2.edge.getLength() - p2.toBasePointDistance);
                }
            }
        }
        return minDis;
    }

    /**
     * Get minimum distance from p1 to p2
     *
     * @param p1
     * @param p2
     * @return
     */
    public double minDistance(MMPoint p1, MMPoint p2) {
        if (p1 == p2) return 0.0;
        if (p1.isTowerPoint && p2.isTowerPoint) return minDisBetweenTowerPoints(p1, p2);
        else if (p1.isTowerPoint && !p2.isTowerPoint) {
            return minDisBetweenTowerPointAndPillarPoint(p1, p2);
        } else if (!p1.isTowerPoint && p2.isTowerPoint) {
            return minDisBetweenPillarPointAndTowerPoint(p1, p2);
        } else {
            return minDisBetweenPillarPoints(p1, p2);
        }
    }

    /**
     * Get a list of points with minimum distance from p1 (exclusive) to p2 (inclusive)
     * the return list excludes p1 and p2
     * Notice that p1 to p1 is p1
     *
     * @param p1
     * @param p2
     * @return
     */
    public List<MMPoint> shortestPath(MMPoint p1, MMPoint p2) {
        List<MMPoint> resPoints = new ArrayList<>();
        if (p1 == p2) return resPoints;
        if (p1.isTowerPoint && p2.isTowerPoint)
            return entryMap.get(p1.hashCode()).routing.get(p2.hashCode());
        else if (p1.isTowerPoint && !p2.isTowerPoint) {
            double minDis = Double.MAX_VALUE;
            if (p2.edge.isForward) {
                Double distance = entryMap.get(p1.hashCode()).dist.get(p2.edge.basePoint.hashCode());
                if (distance != null) {
                    minDis = distance + p2.toBasePointDistance;
                    resPoints.addAll(entryMap.get(p1.hashCode()).routing.get(p2.edge.basePoint.hashCode()));
                    resPoints.add(p2);
                }
            }
            if (p2.edge.isBackward) {
                Double distance = entryMap.get(p1.hashCode()).dist.get(p2.edge.adjPoint.hashCode());
                if (distance != null && minDis > distance + p2.edge.getLength() - p2.toBasePointDistance) {
                    //minDis = Math.min(minDis, distance + p2.edge.getLength() - p2.toBasePointDistance);
                    resPoints.clear();
                    resPoints.addAll(entryMap.get(p1.hashCode()).routing.get(p2.edge.adjPoint.hashCode()));
                    resPoints.add(p2);
                }
            }
        } else if (!p1.isTowerPoint && p2.isTowerPoint) {
            double minDis = Double.MAX_VALUE;
            if (p1.edge.isBackward) {
                Double res = entryMap.get(p1.edge.basePoint.hashCode()).dist.get(p2.hashCode());
                if (res != null) {
                    minDis = res + p1.toBasePointDistance;
                    resPoints.add(p1.edge.basePoint);
                    resPoints.addAll(entryMap.get(p1.edge.basePoint.hashCode()).routing.get(p2.hashCode()));
                }
            }
            if (p1.edge.isForward) {
                Double res = entryMap.get(p1.edge.adjPoint.hashCode()).dist.get(p2.hashCode());
                if (res != null && minDis > res + p1.edge.getLength() - p1.toBasePointDistance) {
                    resPoints.clear();
                    resPoints.add(p1.edge.adjPoint);
                    resPoints.addAll(entryMap.get(p1.edge.adjPoint.hashCode()).routing.get(p2.hashCode()));
                }
            }
        } else {
            if (p1.edge == p2.edge) {
                int flag = 0;
                for (MMPoint point : p1.edge.getPillarPoints()) {
                    if (flag != 0) break;
                    if (point == p1) flag = 1;
                    if (point == p2) flag = 2;
                }
                if ((flag == 1 && p1.edge.isForward) || (flag == 2 && p1.edge.isBackward)) {
                    resPoints.add(p2);
                } else if (flag == 1 && p1.edge.isBackward) {
                    //p1->p1.base->p1.adj->p2
                    Double dist = entryMap.get(p1.edge.basePoint.hashCode()).dist.get(p1.edge.adjPoint.hashCode());
                    if (dist != null) {
                        resPoints.add(p1.edge.basePoint);
                        resPoints.addAll(entryMap.get(p1.edge.basePoint.hashCode()).routing.get(p1.edge.adjPoint.hashCode()));
                        resPoints.add(p2);
                    }
                } else if (flag == 2 && p1.edge.isForward) {
                    //p1->p1.adj->p1.base->p2
                    Double dist = entryMap.get(p1.edge.adjPoint.hashCode()).dist.get(p1.edge.basePoint.hashCode());
                    if (dist != null) {
                        resPoints.add(p1.edge.adjPoint);
                        resPoints.addAll(entryMap.get(p1.edge.adjPoint.hashCode()).routing.get(p1.edge.basePoint.hashCode()));
                        resPoints.add(p2);
                    }
                }
            } else {
                double minDis = Double.MAX_VALUE;
                if (p1.edge.isForward) {
                    //p1->p1.adj->p2.base->p2
                    if (p2.edge.isForward) {
                        Double res = entryMap.get(p1.edge.adjPoint.hashCode()).dist.get(p2.edge.basePoint.hashCode());
                        if (res != null && minDis > res + p1.edge.getLength() - p1.toBasePointDistance + p2.toBasePointDistance) {
                            minDis = res + p1.edge.getLength() - p1.toBasePointDistance + p2.toBasePointDistance;
                            resPoints.add(p1.edge.adjPoint);
                            resPoints.addAll(entryMap.get(p1.edge.adjPoint.hashCode()).routing.get(p2.edge.basePoint.hashCode()));
                            resPoints.add(p2);
                        }
                    }
                    //p1->p1.adj->p2.adj->p2
                    if (p2.edge.isBackward) {
                        Double res = entryMap.get(p1.edge.adjPoint.hashCode()).dist.get(p2.edge.adjPoint.hashCode());
                        if (res != null && minDis > res + p1.edge.getLength() - p1.toBasePointDistance + p2.edge.getLength() - p2.toBasePointDistance) {
                            minDis = res + p1.edge.getLength() - p1.toBasePointDistance + p2.edge.getLength() - p2.toBasePointDistance;
                            resPoints.clear();
                            resPoints.add(p1.edge.adjPoint);
                            resPoints.addAll(entryMap.get(p1.edge.adjPoint.hashCode()).routing.get(p2.edge.adjPoint.hashCode()));
                            resPoints.add(p2);
                        }
                    }
                }
                if (p1.edge.isBackward) {
                    //p1->p1.base->p2.base->p2
                    if (p2.edge.isForward) {
                        Double res = entryMap.get(p1.edge.basePoint.hashCode()).dist.get(p2.edge.basePoint.hashCode());
                        if (res != null && minDis > res + p1.toBasePointDistance + p2.toBasePointDistance) {
                            minDis = res + p1.toBasePointDistance + p2.toBasePointDistance;
                            resPoints.clear();
                            resPoints.add(p1.edge.basePoint);
                            resPoints.addAll(entryMap.get(p1.edge.basePoint.hashCode()).routing.get(p2.edge.basePoint.hashCode()));
                            resPoints.add(p2);
                        }
                    }
                    //p1->p1.base->p2.adj->p2
                    if (p2.edge.isBackward) {
                        Double res = entryMap.get(p1.edge.basePoint.hashCode()).dist.get(p2.edge.adjPoint.hashCode());
                        if (res != null && minDis > res + p1.toBasePointDistance + p2.edge.getLength() - p2.toBasePointDistance) {
                            resPoints.clear();
                            resPoints.add(p1.edge.basePoint);
                            resPoints.addAll(entryMap.get(p1.edge.basePoint.hashCode()).routing.get(p2.edge.adjPoint.hashCode()));
                            resPoints.add(p2);
                        }
                    }
                }
            }
        }
        return resPoints;
    }

    public void addEntry(MMPoint startPoint, Map<Integer, Double> dist, Map<Integer, List<MMPoint>> routing) {
        ShortestPathEntry entry = new ShortestPathEntry(startPoint, dist, routing);
        this.entryMap.put(startPoint.hashCode(), entry);
    }
}

class ShortestPathEntry implements Serializable {

    final MMPoint startPoint;

    /**
     * distance from startPoint to key, key represents endPoint.hashCode()
     */
    final Map<Integer, Double> dist;

    /**
     * route from startPoint to key, key represents endPoint.hashCode()
     */
    final Map<Integer, List<MMPoint>> routing;

    public ShortestPathEntry(MMPoint startPoint, Map<Integer, Double> dist, Map<Integer, List<MMPoint>> routing) {
        this.startPoint = startPoint;
        this.dist = dist;
        this.routing = routing;
    }
}