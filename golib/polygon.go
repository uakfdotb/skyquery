package golib

import (
	"github.com/mitroadmaps/gomapinfer/common"

	"fmt"
	"strconv"
	"strings"
)

func ParsePolygon(s string) common.Polygon {
	if s == "" {
		return common.Polygon{}
	}
	parts := strings.Split(s, " ")
	var polygon common.Polygon
	for _, part := range parts {
		pointParts := strings.Split(part, ",")
		x, _ := strconv.ParseFloat(pointParts[0], 64)
		y, _ := strconv.ParseFloat(pointParts[1], 64)
		polygon = append(polygon, common.Point{x, y})
	}
	return polygon
}

func EncodePolygon(poly common.Polygon) string {
	var pointStrs []string
	for _, p := range poly {
		pointStrs = append(pointStrs, fmt.Sprintf("%v,%v", p.X, p.Y))
	}
	return strings.Join(pointStrs, " ")
}
