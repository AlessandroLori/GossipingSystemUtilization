package model

import "time"

type PowerClass string
const (
  PowerWeak    PowerClass = "weak"
  PowerMedium  PowerClass = "medium"
  PowerStrong  PowerClass = "powerful"
)

type Capacity struct {
  CPU float64 // H/s
  MEM float64 // H/s
  GPU float64 // H/s (0 se assente)
}

type Rates struct {
  CPU float64 // H/s in uso dai job
  MEM float64
  GPU float64
}

type Background struct {
  CPU float64 // H/s (OS/servizi)
  MEM float64
  GPU float64
  // per drift lento (opzionale, li useremo dopo)
  NextTargetAt time.Time
}

type Percentages struct {
  CPU float64 // 0..100 (se GPU assente => GPU=-1)
  MEM float64
  GPU float64
}

