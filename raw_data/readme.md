## raw data for train and test

文件包含二个子目录：country和industry
country和industry下每个子目录就是一个类别
但有的子目录下文件非常少，因此要选择文件比较多的目录（至少二个）进行训练和测试。
例如
NBCorpus\Country\ALB下包含81个文件
NBCorpus\Country\ARG下包含108个文件
NBCorpus\Country\AUSTR下包含305个文件
NBCorpus\Country\BELG下包含154个文件
NBCorpus\Country\BRAZ下包含200个文件
NBCorpus\Country\CANA下包含263个文件

NBCorpus\Industry\I01001下180个文件
NBCorpus\Industry\I13000下325个文件

可以选择country或industry下的几个文件比较多的目录进行训练和测试
例如选择NBCorpus\Country\AUSTR和BCorpus\Country\CANA二个目录，那么分类的class为AUSTER和CANA

目录下的文件可以按一定比例随机挑选出来作为训练样本，剩下的文件作为测试样本。

每个文件已经分好词，一行一个单词。
