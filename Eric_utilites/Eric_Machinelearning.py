


def f_ModelOutcome(clf, X_train, y_train, X_test, y_test, X_V, y_V):
    
    from sklearn.metrics import mean_squared_error,r2_score
    clf.fit(X_train,y_train) 
    y_train_pred = clf.predict(X_train)
    y_test_pred = clf.predict(X_test)
    y_V_pred = clf.predict(X_V)
    
    
    #print('Intercept: %.3f' % clf.intercept_)

    print('MSE train: %.3f, test: %.3f, X_V: %.3f' % (mean_squared_error(y_train, y_train_pred),
                                                     mean_squared_error(y_test, y_test_pred),
                                                     mean_squared_error(y_V, y_V_pred)))

    print('R^2 train: %.3f, test: %.3f, X_V: %.3f' % (r2_score(y_train, y_train_pred),
                                                     r2_score(y_test, y_test_pred),
                                                     r2_score(y_V, y_V_pred)))
    return clf