package plugin

import (
    "database/sql"
    "reflect"
    "strconv"
    "time"
)

func GetDataTypeByType(dataType reflect.Type) string {
    if dataType.AssignableTo(reflect.TypeOf(int64(0))) {
        return "int64"
    } else if dataType.AssignableTo(reflect.TypeOf(float64(0))) {
        return "float64"
    } else if (dataType.AssignableTo(reflect.TypeOf(time.Time{}))) {
        return "time"
    } else {
        return "string"
    }
}

func ConvertValue(val sql.RawBytes, dataType string) any {
    if dataType == "int64" {
        num, _ := strconv.ParseInt(string(val), 10, 64)
        return num
    } else if dataType == "float64" {
        float, _ := strconv.ParseFloat(string(val), 64)
        return float
    } else if dataType == "time" {
        dateVal, _ := time.Parse(time.RFC3339, string(val))
        return dateVal
    } else {
        return string(val)
    }
}

func ConvertValueArray(dataType string, sourceValues []any) any{
    var values any
    if dataType == "int64" {
        values = ConvertSlice[int64](sourceValues, 0)
    } else if dataType == "float64" {
        values = ConvertSlice[float64](sourceValues, 0)
    } else if dataType == "time" {
        values = ConvertSlice[time.Time](sourceValues, time.Time{}.Local())
    } else {
        values = ConvertSlice[string](sourceValues, "")
    }
    return values
}

func ConvertSlice[E any](in []any, nilValue E) (out []E) {
    out = make([]E, 0, len(in))
    for _, v := range in {
        if v != nil {
            out = append(out, v.(E))
        } else {
            out = append(out, nilValue)
        }
    }
    return out
}