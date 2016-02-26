#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include "linear.h"
#define Malloc(type,n) (type *)malloc((n)*sizeof(type))
#define INF HUGE_VAL

//#include <map>
//#include <iterator>

void print_null(const char *s) {}

void exit_with_help()
{
	printf(
			"Usage: train [options] training_set_file [model_file]\n"
			"options:\n"
			//	"-s type : set type of solver (default 1)\n"
			//	"  for multi-class classification\n"
			//	"	 0 -- L2-regularized logistic regression (primal)\n"
			//	"	 1 -- L2-regularized L2-loss support vector classification (dual)\n"
			//	"	 2 -- L2-regularized L2-loss support vector classification (primal)\n"
			//	"	 3 -- L2-regularized L1-loss support vector classification (dual)\n"
			//	"	 4 -- support vector classification by Crammer and Singer\n"
			//	"	 5 -- L1-regularized L2-loss support vector classification\n"
			//	"	 6 -- L1-regularized logistic regression\n"
			//	"	 7 -- L2-regularized logistic regression (dual)\n"
			//	"  for regression\n"
			//	"	11 -- L2-regularized L2-loss support vector regression (primal)\n"
			//	"	12 -- L2-regularized L2-loss support vector regression (dual)\n"
			//	"	13 -- L2-regularized L1-loss support vector regression (dual)\n"
			"	-c cost : set the parameter C (default 1)\n"
			//	"-p epsilon : set the epsilon in loss function of SVR (default 0.1)\n"
			//	"-e epsilon : set tolerance of termination criterion\n"
			//	"	-s 0 and 2\n"
			//	"		|f'(w)|_2 <= eps*min(pos,neg)/l*|f'(w0)|_2,\n"
			//	"		where f is the primal function and pos/neg are # of\n"
			//	"		positive/negative data (default 0.01)\n"
			//	"	-s 11\n"
			//	"		|f'(w)|_2 <= eps*|f'(w0)|_2 (default 0.001)\n"
			//	"	-s 1, 3, 4, and 7\n"
			//	"		Dual maximal violation <= eps; similar to libsvm (default 0.1)\n"
			//	"	-s 5 and 6\n"
			//	"		|f'(w)|_1 <= eps*min(pos,neg)/l*|f'(w0)|_1,\n"
			//	"		where f is the primal function (default 0.01)\n"
			//	"	-s 12 and 13\n"
			//	"		|f'(alpha)|_1 <= eps |f'(alpha0)|,\n"
			//	"		where f is the dual function (default 0.1)\n"
			//	"-B bias : if bias >= 0, instance x becomes [x; bias]; if < 0, no bias term added (default -1)\n"
			//	"-wi weight: weights adjust the parameter C of different classes (see README for details)\n"
			//	"-v n: n-fold cross validation mode\n"
		"	-q : quiet mode (no outputs)\n"
		//	"-W weight_file: set weight file\n"
		"train_set_file format:\n"
		"	Show:Click N:fea1_fea2_fea3_..._feaN\n"
		"	Show:Click N:fea1_fea2_fea3_..._feaN\n"
		);
	exit(1);
}

void exit_input_error(int line_num)
{
	fprintf(stderr,"Wrong input format at line %d\n", line_num);
	exit(1);
}

static char *line = NULL;
static int max_line_len;

static char* readline(FILE *input)
{
	int len;

	if(fgets(line,max_line_len,input) == NULL)
		return NULL;

	while(strrchr(line,'\n') == NULL)
	{
		max_line_len *= 2;
		line = (char *) realloc(line,max_line_len);
		len = (int) strlen(line);
		if(fgets(line+len,max_line_len-len,input) == NULL)
			break;
	}
	return line;
}

void parse_command_line(int argc, char **argv, char *input_file_name, char *model_file_name);
void read_problem(const char *filename);
void read_problem_new(const char *filename);
void read_problem_col(const char *filename);
void do_cross_validation();
void dump_problem(void);
void dump_problem_col(void);

struct feature_node *x_space;
struct parameter param;
struct problem prob;
struct model* model_;
char *weight_file;
int flag_cross_validation;
int nr_fold;
double bias;

//using std::map;
//map<int,int> col_cnt;

int main(int argc, char **argv)
{
	char input_file_name[1024];
	char model_file_name[1024];
	const char *error_msg;

	parse_command_line(argc, argv, input_file_name, model_file_name);

	//read_problem_new(input_file_name);
	//dump_problem();

	read_problem_col(input_file_name);
	//dump_problem_col();

	//exit(0);

	error_msg = check_parameter(&prob,&param);

	if(error_msg)
	{
		fprintf(stderr,"ERROR: %s\n",error_msg);
		exit(1);
	}

	if(flag_cross_validation)
	{
		do_cross_validation();
	}
	else
	{
		model_=train(&prob, &param);
		if(save_model(model_file_name, model_))
		{
			fprintf(stderr,"can't save model to file %s\n",model_file_name);
			exit(1);
		}
		free_and_destroy_model(&model_);
	}

	destroy_param(&param);
	free(prob.y);
	free(prob.x);
	free(prob.W);
	free(x_space);
	free(line);

	return 0;
}

void do_cross_validation()
{
	int i;
	int total_correct = 0;
	double total_error = 0;
	double sumv = 0, sumy = 0, sumvv = 0, sumyy = 0, sumvy = 0;
	double *target = Malloc(double, prob.l);

	cross_validation(&prob,&param,nr_fold,target);
	if(param.solver_type == L2R_L2LOSS_SVR ||
			param.solver_type == L2R_L1LOSS_SVR_DUAL ||
			param.solver_type == L2R_L2LOSS_SVR_DUAL)
	{
		for(i=0;i<prob.l;i++)
		{
			double y = prob.y[i];
			double v = target[i];
			total_error += (v-y)*(v-y);
			sumv += v;
			sumy += y;
			sumvv += v*v;
			sumyy += y*y;
			sumvy += v*y;
		}
		printf("Cross Validation Mean squared error = %g\n",total_error/prob.l);
		printf("Cross Validation Squared correlation coefficient = %g\n",
				((prob.l*sumvy-sumv*sumy)*(prob.l*sumvy-sumv*sumy))/
				((prob.l*sumvv-sumv*sumv)*(prob.l*sumyy-sumy*sumy))
			  );
	}
	else
	{
		for(i=0;i<prob.l;i++)
			if(target[i] == prob.y[i])
				++total_correct;
		printf("Cross Validation Accuracy = %g%%\n",100.0*total_correct/prob.l);
	}

	free(target);
}

void parse_command_line(int argc, char **argv, char *input_file_name, char *model_file_name)
{
	int i;
	void (*print_func)(const char*) = NULL;	// default printing to stdout

	// default values
	// param.solver_type = L2R_L2LOSS_SVC_DUAL;
	param.solver_type = L1R_LR;
	param.C = 1;
	param.eps = INF; // see setting below
	param.p = 0.1;
	param.nr_weight = 0;
	param.weight_label = NULL;
	param.weight = NULL;
	flag_cross_validation = 0;
	weight_file = NULL;
	bias = -1;

	float penalty_weight = 1.0;

	// parse options
	for(i=1;i<argc;i++)
	{
		if(argv[i][0] != '-') break;
		if(++i>=argc)
			exit_with_help();
		switch(argv[i-1][1])
		{
			case 's':
				param.solver_type = atoi(argv[i]);
				break;

			case 'c':
				penalty_weight = (float)atof(argv[i]);
				if(penalty_weight<0)
				{
					fprintf(stderr, "penalty factor weight C must > 0\n");
					exit_with_help();
				}
				param.C = 1.0 / penalty_weight;
				//fprintf(stderr, "%g %g\n", 1 / penalty_weight, 1.0 / penalty_weight);
				break;

			case 'p':
				param.p = atof(argv[i]);
				break;

			case 'e':
				param.eps = atof(argv[i]);
				break;

			case 'B':
				bias = atof(argv[i]);
				break;

			case 'w':
				++param.nr_weight;
				param.weight_label = (int *) realloc(param.weight_label,sizeof(int)*param.nr_weight);
				param.weight = (double *) realloc(param.weight,sizeof(double)*param.nr_weight);
				param.weight_label[param.nr_weight-1] = atoi(&argv[i-1][2]);
				param.weight[param.nr_weight-1] = atof(argv[i]);
				break;

			case 'v':
				flag_cross_validation = 1;
				nr_fold = atoi(argv[i]);
				if(nr_fold < 2)
				{
					fprintf(stderr,"n-fold cross validation: n must >= 2\n");
					exit_with_help();
				}
				break;

			case 'q':
				print_func = &print_null;
				i--;
				break;

			case 'W':
				weight_file = argv[i];
				break;

			default:
				fprintf(stderr,"unknown option: -%c\n", argv[i-1][1]);
				exit_with_help();
				break;
		}
	}

	set_print_string_function(print_func);

	// determine filenames
	if(i>=argc)
		exit_with_help();

	strcpy(input_file_name, argv[i]);

	if(i<argc-1)
		strcpy(model_file_name,argv[i+1]);
	else
	{
		char *p = strrchr(argv[i],'/');
		if(p==NULL)
			p = argv[i];
		else
			++p;
		sprintf(model_file_name,"%s.model",p);
	}

	if(param.eps == INF)
	{
		switch(param.solver_type)
		{
			case L2R_LR:
			case L2R_L2LOSS_SVC:
				param.eps = 0.01;
				break;
			case L2R_L2LOSS_SVR:
				param.eps = 0.001;
				break;
			case L2R_L2LOSS_SVC_DUAL:
			case L2R_L1LOSS_SVC_DUAL:
			case MCSVM_CS:
			case L2R_LR_DUAL:
				param.eps = 0.1;
				break;
			case L1R_L2LOSS_SVC:
			case L1R_LR:
				param.eps = 0.01;
				break;
			case L2R_L1LOSS_SVR_DUAL:
			case L2R_L2LOSS_SVR_DUAL:
				param.eps = 0.1;
				break;
		}
	}
}

// Show:Click N:fea1_fea2_fea3_..._feaN
// Show:Click N:fea1_fea2_fea3_..._feaN
void read_problem_col(const char *filename)
{
	int max_index, inst_max_index, i;
	size_t elements;//, j;
	FILE *fp = fopen(filename,"r");
	char *endptr;
	//	char *idx, *val, *label;

	if(fp == NULL)
	{
		fprintf(stderr,"can't open input file %s\n",filename);
		exit(1);
	}

	prob.l = 0;
	elements = 0;
	max_line_len = 10240;
	line = Malloc(char,max_line_len);

    int max_fea_cnt = 1024 * 1024 * 1024;
    size_t *col_cnt = Malloc(size_t, max_fea_cnt);
    //memset(col_cnt, 0, 1024*1024*1024*sizeof(unsigned int));
    for (int i = 0; i < 1024 * 1024 * 1024; i++)
        col_cnt[i] = 0;

    size_t nzz = 0;
	max_index = 0;
	while(readline(fp)!=NULL)
	{
		int show = 0;
		int click = 0;
		int fea_num = 0;
		inst_max_index = 0;

		char *p = strtok(line,": \t"); // neg label
		show = (int)strtol(p, &endptr, 10);

		p = strtok(NULL, ": \t");  // pos lablel
		click = (int)strtol(p, &endptr, 10);

		// features
		p = strtok(NULL, ": \t");
		fea_num = (int)strtol(p, &endptr, 10);

		elements += fea_num;
		prob.l++;

		if (click > 0 && show > click)
		{
			//elements += fea_num;
			//elements++;
			prob.l++;
		}

        int idx = 0;
		while(1)
		{
			p = strtok(NULL,"_");
			//val = strtok(NULL," \t");

			if(p == NULL)
				break;

			//errno = 0;
			idx = (int) strtol(p, &endptr, 10);
			if (idx <= inst_max_index)
			{
				fprintf(stderr, "feature number should be asending");
				exit_input_error(prob.l);
			}

            col_cnt[idx]++;
            nzz++;
//			if (col_cnt.find(idx) == col_cnt.end())
//			{
//				col_cnt[idx] = 1;
//                nzz++;
//			}
//			else
//            {
//				col_cnt[idx]++;
//                nzz++;
//            }

			if (click > 0 && show > click)
            {
				col_cnt[idx]++;
                nzz++;
            }
            if (idx > max_index)
                max_index = idx;
		}
	}
	rewind(fp);


	fprintf(stderr, "prob.n=%d prob.l=%d\n", max_index, prob.l);

	prob.bias=bias;

	prob.y = Malloc(double,prob.l);
	prob.x = Malloc(struct feature_node *, max_index + 1);
	prob.W = Malloc(double,prob.l);
	x_space = Malloc(struct feature_node, nzz + max_index + 1);

	for (int i = 1; i < max_index + 1; i++)
	{
        //fprintf(stderr, "%d, %d\n", it, col_cnt[it]);
		col_cnt[i] += (col_cnt[i-1] + 1);
        //fprintf(stderr, "%d, %d\n", it, col_cnt[it]);
	}

    for (int i = 0; i < max_index; i++)
    {
		prob.x[i] = &x_space[col_cnt[i]];
    }

    fprintf(stderr, "finish col matrix initial. debug info [%ld|nzz=%ld].\n", col_cnt[max_index], nzz);
    //fprintf(stderr, "col[0]=%d, col[1]=%d, col[2]=%d\n", col_cnt[0], col_cnt[1], col_cnt[2]);

	//j=0;
	for(i=0;i<prob.l;i++)
	{
		inst_max_index = 0; // strtol gives 0 if wrong format
		readline(fp);

		int show = 0;
		int click = 0;
		int fea_num = 0;

		char *p = strtok(line,": \t"); // neg label
		show = (int)strtol(p, &endptr, 10);

		p = strtok(NULL, ": \t");  // pos lablel
		click = (int)strtol(p, &endptr, 10);

		// features
		p = strtok(NULL, ": \t");
		fea_num = (int)strtol(p, &endptr, 10);

		//if(label == NULL) // empty line
		//	exit_input_error(i+1);

		int neg = 0;
		if (show < click)
        {
			neg = 0;
        }
		else
			neg = show - click;

		if (neg == 0 && click == 0)
		{
			fprintf(stderr,"label error :");
			exit_input_error(i+1);
		}

		if (neg > 0)
		{
			prob.y[i] = -1;
			prob.W[i] = neg;

			if (click > 0)
			{
				prob.y[i+1] = +1;
				prob.W[i+1] = click;
				++i;
			}
		}
		else // neg = 0
		{
			prob.y[i] = +1;
			prob.W[i] = click;
			//fprintf(stdout, "%d\n", click);
		}

		int index = 0;
		while(1)
		{
			p = strtok(NULL,"_");

			if(p == NULL)
				break;

			index = (int) strtol(p,&endptr,10);

            int ind = index - 1;

//            if (index < 1 || index > max_index)
//            {
//                fprintf(stderr, "wrong feature %d:%d:%d\n", index, col_cnt[ind], i);
//                continue;
//            }
            
			if (neg > 0 && click > 0)
			{
                //fprintf(stderr, "%d:%d:%d ", index, col_cnt[ind], i);
				x_space[col_cnt[ind]].index = i;
				col_cnt[ind]++;
			}

            //fprintf(stderr, "%d:%d:%d ", index, col_cnt[ind], i+1);
			x_space[col_cnt[ind]].index = i + 1;
			col_cnt[ind]++;

		}
        //fprintf(stderr, " ---- %d \n", i);
	}

    fprintf(stderr, "finish col cell fullfill\n");

    //fprintf(stderr, "-1: ");
	for (int it = 0; it <= max_index; it++)
    {
		x_space[col_cnt[it]].index = -1;
        //fprintf(stderr, "%d:%d ", it->first, it->second);
    }
    //fprintf(stderr,"\n");

    fprintf(stderr, "finish col board fullfill\n");

	if(prob.bias >= 0)
	{
		prob.n=max_index+1;
		for(i=1;i<prob.l;i++)
			(prob.x[i]-2)->index = prob.n;
		//x_space[j-2].index = prob.n;
	}
	else
		prob.n=max_index;

	fprintf(stderr, "feature.dimension=%d\n", prob.n);
    /* dump problem */
//	for(int i=0; i<prob.n; i++)
//	{
//		struct feature_node *node = prob.x[i];
//		fprintf(stdout, "%d:%d@%p", i, col_cnt[i], node);
//		while(node->index != -1)
//		{
//			fprintf(stdout, " %d", node->index);
//			++node;
//		}
//		fprintf(stdout, "\n");
//	}

    free(col_cnt);

	fclose(fp);
	/*
	   if(weight_file) 
	   {
	   fp = fopen(weight_file,"r");
	   for(i=0;i<prob.l;i++)
	   fscanf(fp,"%lf",&prob.W[i]);
	   fclose(fp);
	   }
	   else
	   {
	   for(i=0;i<prob.l;i++)
	   prob.W[i] = 1;
	   }
	   */
}


// Show:Click N:fea1_fea2_fea3_..._feaN
// Show:Click N:fea1_fea2_fea3_..._feaN
void read_problem_new(const char *filename)
{
	int max_index, inst_max_index, i;
	size_t elements, j;
	FILE *fp = fopen(filename,"r");
	char *endptr;
	//	char *idx, *val, *label;

	if(fp == NULL)
	{
		fprintf(stderr,"can't open input file %s\n",filename);
		exit(1);
	}

	prob.l = 0;
	elements = 0;
	max_line_len = 10240;
	line = Malloc(char,max_line_len);
	while(readline(fp)!=NULL)
	{
		int show = 0;
		int click = 0;
		int fea_num = 0;

		char *p = strtok(line,": \t"); // neg label
		show = (int)strtol(p, &endptr, 10);

		p = strtok(NULL, ": \t");  // pos lablel
		click = (int)strtol(p, &endptr, 10);

		// features
		p = strtok(NULL, ": \t");
		fea_num = (int)strtol(p, &endptr, 10);

		elements += fea_num;
		/*
		   while(1)
		   {
		   p = strtok(NULL,": \t");
		   if(p == NULL || *p == '\n') // check '\n' as ' ' may be after the last feature
		   break;
		   elements++;
		   }
		   */
		elements++; // for bias term
		prob.l++;

		if (click > 0 && show > click)
		{
			//elements += fea_num;
			//elements++;
			prob.l++;
		}

	}
	rewind(fp);

	fprintf(stderr, "element=%ld prob.l=%d\n", elements, prob.l);

	prob.bias=bias;

	prob.y = Malloc(double,prob.l);
	prob.x = Malloc(struct feature_node *,prob.l);
	prob.W = Malloc(double,prob.l);
	x_space = Malloc(struct feature_node, elements + prob.l);

	max_index = 0;
	j=0;
	for(i=0;i<prob.l;i++)
	{
		inst_max_index = 0; // strtol gives 0 if wrong format
		readline(fp);
		prob.x[i] = &x_space[j];

		//label = strtok(line," \t\n");

		int show = 0;
		int click = 0;
		int fea_num = 0;

		char *p = strtok(line,": \t"); // neg label
		show = (int)strtol(p, &endptr, 10);

		p = strtok(NULL, ": \t");  // pos lablel
		click = (int)strtol(p, &endptr, 10);

		// features
		p = strtok(NULL, ": \t");
		fea_num = (int)strtol(p, &endptr, 10);

		//if(label == NULL) // empty line
		//	exit_input_error(i+1);

		int neg = 0;
		if (show < click)
			neg = 0;
		else
			neg = show - click;

		if (neg == 0 && click == 0)
		{
			fprintf(stderr,"label error :");
			exit_input_error(i+1);
		}

		if (neg > 0)
		{
			prob.y[i] = -1;
			prob.W[i] = neg;

			//fprintf(stdout, "%d\n", neg);

			if (click > 0)
			{
				prob.x[i+1] = &x_space[j];
				prob.y[i+1] = +1;
				prob.W[i+1] = click;
				++i;
				//fprintf(stdout, "%d\n", click);
			}
		}
		else // neg = 0
		{
			prob.y[i] = +1;
			prob.W[i] = click;
			//fprintf(stdout, "%d\n", click);
		}

		//if(endptr == label || *endptr != '\0')
		//	exit_input_error(i+1);


		//fprintf(stdout, "neg=%d pos=%d fea_num=%d\n", neg, click, fea_num);

		int num = 0;
		while(++num)
		{
			p = strtok(NULL,"_");
			//val = strtok(NULL," \t");

			if(p == NULL)
				break;

			//errno = 0;
			x_space[j].index = (int) strtol(p,&endptr,10);
			//if(endptr == p || *endptr != '\0' || x_space[j].index <= inst_max_index)
			//{
			//    fprintf(stdout, "p=%s *endptr=%c index=%d\n", p, errno, *endptr, x_space[j].index);
			//	exit_input_error(i+1);
			//}
			//else
			if (x_space[j].index <= inst_max_index) // feature number is asending
			{
				fprintf(stderr,"feature error [%d,%d] :", x_space[j].index, inst_max_index);
				exit_input_error(i+1);
			}

			inst_max_index = x_space[j].index;

			errno = 0;
			//x_space[j].value = strtod(val,&endptr);
			//x_space[j].value = 1;
			//if(endptr == val || errno != 0 || (*endptr != '\0' && !isspace(*endptr)))
			//	exit_input_error(i+1);

			++j;
		}

		if(inst_max_index > max_index)
			max_index = inst_max_index;

		if(prob.bias >= 0)
			j++;
		//			x_space[j++].value = prob.bias;

		x_space[j++].index = -1;
	}

	if(prob.bias >= 0)
	{
		prob.n=max_index+1;
		for(i=1;i<prob.l;i++)
			(prob.x[i]-2)->index = prob.n;
		x_space[j-2].index = prob.n;
	}
	else
		prob.n=max_index;

	fprintf(stderr, "prob.n=%d\n", prob.n);

	fclose(fp);
	/*
	   if(weight_file) 
	   {
	   fp = fopen(weight_file,"r");
	   for(i=0;i<prob.l;i++)
	   fscanf(fp,"%lf",&prob.W[i]);
	   fclose(fp);
	   }
	   else
	   {
	   for(i=0;i<prob.l;i++)
	   prob.W[i] = 1;
	   }
	   */
}

void dump_problem()
{
	for(int i=0; i<prob.l; i++)
	{
		//fprintf(stdout, "label=%f weight=%f : ", prob.y[i], prob.W[i]);
		if ((int)prob.y[i] == 1)
			fprintf(stdout, "+1");
		else
			fprintf(stdout, "-1");
		struct feature_node *node = prob.x[i];
		while(node->index != -1)
		{
			fprintf(stdout, " %d:1", node->index);
			++node;
		}
		fprintf(stdout, "\n");
	}
}

// read in a problem (in libsvm format)
void read_problem(const char *filename)
{
	int max_index, inst_max_index, i;
	size_t elements, j;
	FILE *fp = fopen(filename,"r");
	char *endptr;
	char *idx, *val, *label;

	if(fp == NULL)
	{
		fprintf(stderr,"can't open input file %s\n",filename);
		exit(1);
	}

	prob.l = 0;
	elements = 0;
	max_line_len = 1024;
	line = Malloc(char,max_line_len);
	while(readline(fp)!=NULL)
	{
		char *p = strtok(line," \t"); // label

		// features
		while(1)
		{
			p = strtok(NULL," \t");
			if(p == NULL || *p == '\n') // check '\n' as ' ' may be after the last feature
				break;
			elements++;
		}
		elements++; // for bias term
		prob.l++;
	}
	rewind(fp);

	prob.bias=bias;

	prob.y = Malloc(double,prob.l);
	prob.x = Malloc(struct feature_node *,prob.l);
	prob.W = Malloc(double,prob.l);
	x_space = Malloc(struct feature_node,elements+prob.l);

	max_index = 0;
	j=0;
	for(i=0;i<prob.l;i++)
	{
		inst_max_index = 0; // strtol gives 0 if wrong format
		readline(fp);
		prob.x[i] = &x_space[j];
		label = strtok(line," \t\n");
		if(label == NULL) // empty line
			exit_input_error(i+1);

		prob.y[i] = strtod(label,&endptr);
		if(endptr == label || *endptr != '\0')
			exit_input_error(i+1);

		while(1)
		{
			idx = strtok(NULL,":");
			val = strtok(NULL," \t");

			if(val == NULL)
				break;

			errno = 0;
			x_space[j].index = (int) strtol(idx,&endptr,10);
			if(endptr == idx || errno != 0 || *endptr != '\0' || x_space[j].index <= inst_max_index)
				exit_input_error(i+1);
			else
				inst_max_index = x_space[j].index;

			errno = 0;
			//			x_space[j].value = strtod(val,&endptr);
			if(endptr == val || errno != 0 || (*endptr != '\0' && !isspace(*endptr)))
				exit_input_error(i+1);

			++j;
		}

		if(inst_max_index > max_index)
			max_index = inst_max_index;

		if(prob.bias >= 0)
			j++;
		//x_space[j++].value = prob.bias;

		x_space[j++].index = -1;
	}

	if(prob.bias >= 0)
	{
		prob.n=max_index+1;
		for(i=1;i<prob.l;i++)
			(prob.x[i]-2)->index = prob.n;
		x_space[j-2].index = prob.n;
	}
	else
		prob.n=max_index;

	fclose(fp);

	if(weight_file) 
	{
		fp = fopen(weight_file,"r");
		for(i=0;i<prob.l;i++)
			fscanf(fp,"%lf",&prob.W[i]);
		fclose(fp);
	}
	else
	{
		for(i=0;i<prob.l;i++)
			prob.W[i] = 1;
	}
}
