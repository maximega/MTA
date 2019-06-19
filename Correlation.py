import scipy.stats

def Correlation(x,y):
	print("(Correlation coeffecient, p_vale) = ", scipy.stats.pearsonr(x, y))