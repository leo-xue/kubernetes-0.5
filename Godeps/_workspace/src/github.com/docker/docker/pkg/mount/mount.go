package mount

func GetMounts() ([]*MountInfo, error) {
	return parseMountTable()
}
