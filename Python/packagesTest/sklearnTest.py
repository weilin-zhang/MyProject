from sklearn.ensemble import BaggingClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import cross_val_score
from sklearn.datasets import make_blobs
from sklearn.tree import DecisionTreeClassifier
from matplotlib import pyplot


# X = [[0, 0], [1, 1]]
# Y = [0, 1]
# clf = RandomForestClassifier(n_estimators=10)
# clf = clf.fit(X, Y)
# print(clf)



#n_samples:总样本数量，n_features 每个样本的特征数，centers：类别数
X, y = make_blobs(n_samples=10000, n_features=10, centers=100,
    random_state=0)
pyplot.scatter(X[:,0],X[:,1],c=y);
pyplot.show()


#max_features：划分时考虑的最大特征数，max_depth：决策树最大树深
# clf = DecisionTreeClassifier(max_depth=None, min_samples_split=2,
#     random_state=0)
# scores = cross_val_score(clf, X, y)
# scores.mean()


# clf = RandomForestClassifier(n_estimators=10, max_depth=None,
#     min_samples_split=2, random_state=0)
# scores = cross_val_score(clf, X, y)
# scores.mean()

clf = ExtraTreesClassifier(n_estimators=10, max_depth=None,
    min_samples_split=2, random_state=0)
scores = cross_val_score(clf, X, y)
scores.mean() > 0.999

print(scores.mean())
