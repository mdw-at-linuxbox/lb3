/*
 * ref,
 *	aws-doc-sdk-examples
 *	cpp/example_code/s3/put_object_async.cpp
 *
 * creds, one of these choices,
 *	1, set ~/.aws/credentials
 *	2, point env AWS_SHARED_CREDENTIALS_FILE to credentials elsewhere
 *	3, set AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
 * and pass -e = radosgw (unless you want to test against amazon)
 *
 * todo,
 *	must be a way to avoid allocating a possibly
 *	large fixed_size_buf of all 0's.
 */

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/Bucket.h>
#include <network/uri.hpp>
#include <fstream>
#include <iostream>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>

const char *my_endpoint;
const char *my_region;
const char *my_container;

int nt = 1;
int wflag;
int Wflag;
int vflag;
int fixed_size = 0;

char *fixed_size_buf;

int exitcode;

pthread_t *worker_ids;

struct worker_result {
	int r;
};

pthread_mutex_t exists_mutex;
pthread_cond_t exists_cond;
#define NH 55
struct exists_entry {
	struct exists_entry *next;
	int exists;
	int want;
	char fn[1];
} *exists_hash[NH];

int
compute_exists_hash(char *fn)
{
	unsigned r = 0;
	char *cp;
	for (cp = fn; *cp; ++cp) {
		r *= 5;
		r ^= *cp;
	}
	r %= NH;
	return (int) r;
}

void
wait_until_exists(char *fn)
{
	int nh;
	nh = compute_exists_hash(fn);
	struct exists_entry *ep, **epp;
	if (pthread_mutex_lock(&exists_mutex) < 0) {
		fprintf(stderr,"lock failed %d\n", errno);
	}
	for (;;) {
		for (epp = exists_hash + nh; ep = *epp; epp = &ep->next) {
			if (!strcmp(ep->fn, fn)) break;
		}
		if (!ep) {
			ep = (struct exists_entry *) malloc(sizeof *ep + strlen(fn));
			memset(ep, 0, sizeof *ep);
			strcpy(ep->fn, fn);
			ep->want = 1;
			ep->next = *epp;
			*epp = ep;
			continue;
		}
		if (ep->exists) break;
		if (pthread_cond_wait(&exists_cond, &exists_mutex) < 0) {
			fprintf(stderr,"cond wait failed %d\n", errno);
		}
	}
	if (pthread_mutex_unlock(&exists_mutex) < 0) {
		fprintf(stderr,"unlock failed %d\n", errno);
	}
}

void
mark_it_exists(char *fn)
{
	int nh;
	nh = compute_exists_hash(fn);
	struct exists_entry *ep, **epp;

	if (pthread_mutex_lock(&exists_mutex) < 0) {
		fprintf(stderr,"lock failed %d\n", errno);
	}
	for (epp = exists_hash + nh; ep = *epp; epp = &ep->next) {
		if (!strcmp(ep->fn, fn)) break;
	}
	if (!ep) {
		ep = (struct exists_entry *) malloc(sizeof *ep + strlen(fn));
		memset(ep, 0, sizeof *ep);
		strcpy(ep->fn, fn);
		ep->next = *epp;
		*epp = ep;
	}
	ep->exists = 1;
	if (ep->want) {
		pthread_cond_broadcast(&exists_cond);
	}
	if (pthread_mutex_unlock(&exists_mutex) < 0) {
		fprintf(stderr,"unlock failed %d\n", errno);
	}
}

#define W_ADD 1
#define W_DEL 2
#define W_MKB 3
#define W_RMB 4
struct work_element {
	int op;
	char *what;
	struct work_element *next;
};

struct work_element *work_queue;
pthread_mutex_t work_mutex;

int
read_in_data()
{
	char line[512];
	char *cp;
	int lineno;
	char *ep, *s, *q;
	char *op;
	char *what;
	struct work_element *wp;
	struct work_element **wpp;
	int i;
	int r = 0;

	lineno = 0;
	wpp = &work_queue;
	while (fgets(line, sizeof line, stdin)) {
		++lineno;
		cp = strchr(line, '\n');
		if (cp) *cp = 0;
		what = op = NULL;
		for (s = line;
			(q = strtok_r(s, " \t", &ep))!= NULL;
			s = 0) {
			if (!op) op = q;
			else if (!what) what = q;
			else {
				fprintf (stderr, "Extra data not understood: <%s>\n",
					q);
				return 1;
			}
		}
		if (!op) {
			fprintf (stderr,"Missing op at data line %d\n", lineno);
			return 1;
		}
		if (!strcmp(op, "ADD"))
			i = W_ADD;
		else if (!strcmp(op, "DEL"))
			i = W_DEL;
		else if (!strcmp(op, "MKB"))
			i = W_MKB;
		else if (!strcmp(op, "RMB"))
			i = W_RMB;
		else {
			fprintf (stderr,"Bad op <%s> at data line %d\n", op, lineno);
			r = 1;
			continue;
		}
		if (!what) {
			fprintf (stderr,"Missing arg at data line %d\n", lineno);
			r = 1;
			continue;
		}
		cp = (char *) malloc(sizeof *wp + 1 + strlen(what));
		wp = (struct work_element *) cp;
		cp += sizeof *wp;
		memset(wp, 0, sizeof*wp);
		wp->op = i;
		wp->what = cp;
		strcpy(cp, what);
		*wpp = wp;
		wpp = &wp->next;
	}
	return r;
}

struct make_data_arg : public Aws::Client::AsyncCallerContext {
	std::shared_ptr<Aws::IOStream> input_data;

	std::mutex upload_mutex;
	std::condition_variable upload_variable;
	int r;

	struct work_element *work;

	make_data_arg() : r(0)
	{
	}
};

int
cpp_open_file(struct make_data_arg *makedataarg, char *fn)
{
	if (fixed_size) {
		makedataarg->input_data =
			Aws::MakeShared<std::basic_stringstream< char, std::char_traits< char > > >("SampleAllocationTag",
				std::string(fixed_size_buf, fixed_size),
				std::ios_base::in | std::ios_base::binary);
		return 0;
	}
	try {
		auto ip =
			Aws::MakeShared<Aws::FStream>("SampleAllocationTag",
				fn,
				std::ios_base::in | std::ios_base::binary);
		if (!ip->is_open()) {
			return 1;
		}
		makedataarg->input_data = ip;
	} catch (std::runtime_error& e) {
		return 1;
	}
	return 0;
}

void
cpp_cleanup_file(struct make_data_arg *makedataarg)
{
}

void put_object_async_finished(const Aws::S3::S3Client* client,
    const Aws::S3::Model::PutObjectRequest& request,
    const Aws::S3::Model::PutObjectOutcome& outcome,
    const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context)
{
	auto x{dynamic_cast<const make_data_arg*>(context.get())};
	auto y{const_cast<make_data_arg*>(x)};
	std::unique_lock<std::mutex> lock(y->upload_mutex);
	y->upload_variable.notify_one();
	if (!outcome.IsSuccess()) {
		std::cerr << "PutObject failed on " << y->work->what << ": " <<
			outcome.GetError().GetExceptionName() << "" <<
			outcome.GetError().GetMessage() << std::endl;
		y->r = 1;
	}
}

void create_bucket_async_finished(const Aws::S3::S3Client* client,
    const Aws::S3::Model::CreateBucketRequest& request,
    const Aws::S3::Model::CreateBucketOutcome& outcome,
    const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context)
{
	auto x{dynamic_cast<const make_data_arg*>(context.get())};
	auto y{const_cast<make_data_arg*>(x)};
	std::unique_lock<std::mutex> lock(y->upload_mutex);
	y->upload_variable.notify_one();
	if (!outcome.IsSuccess()) {
		std::cerr << "CreateBucket failed on " << y->work->what << ": " <<
			outcome.GetError().GetExceptionName() << "" <<
			outcome.GetError().GetMessage() << std::endl;
		y->r = 1;
	}
}

void delete_bucket_async_finished(const Aws::S3::S3Client* client,
    const Aws::S3::Model::DeleteBucketRequest& request,
    const Aws::S3::Model::DeleteBucketOutcome& outcome,
    const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context)
{
	auto x{dynamic_cast<const make_data_arg*>(context.get())};
	auto y{const_cast<make_data_arg*>(x)};
	std::unique_lock<std::mutex> lock(y->upload_mutex);
	y->upload_variable.notify_one();
	if (!outcome.IsSuccess()) {
		std::cerr << "DeleteBucket failed on " << y->work->what << ": " <<
			outcome.GetError().GetExceptionName() << "" <<
			outcome.GetError().GetMessage() << std::endl;
		y->r = 1;
	}
}

void delete_object_async_finished(const Aws::S3::S3Client* client,
    const Aws::S3::Model::DeleteObjectRequest& request,
    const Aws::S3::Model::DeleteObjectOutcome& outcome,
    const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context)
{
	auto x{dynamic_cast<const make_data_arg*>(context.get())};
	auto y{const_cast<make_data_arg*>(x)};
	std::unique_lock<std::mutex> lock(y->upload_mutex);
	y->upload_variable.notify_one();
	if (!outcome.IsSuccess()) {
		std::cerr << "DeleteObject failed on " << y->work->what << ": " <<
			outcome.GetError().GetExceptionName() << "" <<
			outcome.GetError().GetMessage() << std::endl;
		y->r = 1;
	}
}

struct worker_arg {
	Aws::Client::ClientConfiguration clientconfig;
	worker_arg(Aws::Client::ClientConfiguration &c) : clientconfig(c) {
	}
};

void *
worker_thread(void *a)
{
	int r;
	struct worker_result *wr = new worker_result;
	struct work_element *wp;
	struct work_element **wpp;
	int first;
	struct worker_arg *wa = (struct worker_arg *) a;
	auto makedataarg = new make_data_arg;
	Aws::S3::S3Client s3_client(wa->clientconfig);
	Aws::S3::Model::PutObjectRequest object_request;
	Aws::S3::Model::CreateBucketRequest crbucket_request;
	Aws::S3::Model::DeleteBucketRequest delbucket_request;
	Aws::S3::Model::DeleteObjectRequest delobject_request;
	std::shared_ptr<Aws::Client::AsyncCallerContext> context { makedataarg };

	for (first = 1;;first = 0) {
//		maybe_refresh_token();
		if (pthread_mutex_lock(&work_mutex) < 0) {
			fprintf(stderr,"lock failed %d\n", errno);
		}
		for (wpp = & work_queue; wp = *wpp; ) {
			*wpp = wp->next;
			break;
		}
		if (pthread_mutex_unlock(&work_mutex) < 0) {
			fprintf(stderr,"unlock failed %d\n", errno);
		}
		if (!wp) break;
		switch (wp->op) {
		case W_RMB:
			if (!wflag) break;
		case W_DEL:
			if (Wflag) break;
			wait_until_exists(wp->what);
		}
		std::unique_lock<std::mutex> lock(makedataarg->upload_mutex);
		makedataarg->work = wp;
		switch (wp->op) {
		case W_DEL:
if (vflag) printf ("deleting %s\n", wp->what);
			delobject_request.WithBucket(my_container).WithKey(wp->what);
			s3_client.DeleteObjectAsync(delobject_request, delete_object_async_finished, context);
			break;
		case W_RMB:
if (vflag) printf ("remove bucket %s\n", wp->what);
			delbucket_request.SetBucket(wp->what);
			s3_client.DeleteBucketAsync(delbucket_request, delete_bucket_async_finished, context);
			break;
		case W_ADD:
if (vflag) printf ("adding %s\n", wp->what);
			if (cpp_open_file(makedataarg, wp->what)) {
				fprintf(stderr,"Can't open %s\n", wp->what);
				continue;
			}
			object_request.SetBucket(my_container);
			object_request.SetKey(wp->what);
			object_request.SetBody(makedataarg->input_data);
			s3_client.PutObjectAsync(object_request,
				put_object_async_finished,
				context);
			cpp_cleanup_file(makedataarg);
			break;
		case W_MKB:
if (vflag) printf ("add bucket %s\n", wp->what);
			crbucket_request.SetBucket(wp->what);
			s3_client.CreateBucketAsync(crbucket_request, create_bucket_async_finished, context);
			break;
		default:
			fprintf(stderr,"op %d? for fn %s\n", wp->op, wp->what);
			continue;
		}
		makedataarg->upload_variable.wait(lock);
		if (wp->op == W_ADD) {
			mark_it_exists(wp->what);
		}
		free(wp);
//		if ((r = makedataarg->r)) break;
	}
Done:
//	release_curl_handle(ca);
	if (r) {
		wr->r = r;
	} else {
		delete wr;
		wr = 0;
	}
	return wr;
}

void
start_threads(worker_arg *wa)
{
	int i;
	worker_ids = (pthread_t *) malloc(nt * sizeof *worker_ids);
	for (i = 0; i < nt; ++i)
		pthread_create(worker_ids + i, NULL, worker_thread, wa);
}

void
wait_for_completion()
{
	int i;
	void *result;
	int r;
	struct worker_result *wr;
	r = 0;
	for (i = 0; i < nt; ++i) {
		if (pthread_join(worker_ids[i], &result) < 0) {
			fprintf(stderr,"pthread_join failed %d\n", errno);
			return;
		}
		wr = (worker_result *) result;
		if (wr) {
			r |= wr->r;
			delete wr;
		}
	}
	free(worker_ids);
	worker_ids = 0;
}

void report_results()
{
}

int process()
{
	Aws::Client::ClientConfiguration clientconfig;
	int r;

	if ((r = read_in_data())) {
		std::cerr << "read_in_data failed" << std::endl;
		return r;
	}
	if (my_region)
		clientconfig.region = my_region;
	if (my_endpoint) {
		Aws::String aws_my_endpoint(my_endpoint);
		network::uri u(my_endpoint);
		auto us = u.scheme().to_string();
		if (!us.compare("http")) {
			clientconfig.scheme = Aws::Http::Scheme::HTTP;
		}
		else if (!us.compare("https")) {
			clientconfig.scheme = Aws::Http::Scheme::HTTPS;
		} else {
			throw std::out_of_range("invalid scheme " + us);
		}
		std::string ur;
		ur = u.host().to_string();
		if (u.has_port()) {
			ur += ":" + u.port().to_string();
		}
		clientconfig.endpointOverride = ur.c_str();
	}

	struct worker_arg wa[1]{clientconfig};
	start_threads(wa);
	wait_for_completion();
	report_results();
	return r;
}

int main(int ac, char **av)
{
	char *ap;
	char *cp, *ep;
	while (--ac > 0) if (*(ap = *++av) == '-') while (*++ap) switch(*ap) {
	case 'e':
		if (--ac <= 0) {
			std::cerr << "-e: missing endpoint " << std::endl;
			goto Usage;
		}
		my_endpoint = *++av;
		my_region = "mexico";
		break;
	case 'b':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		my_container = *++av;
		break;
        case 's':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		cp = *++av;
		fixed_size = strtoll(cp, &ep, 0);
		if (cp == ep || *ep) {
			fprintf(stderr,"Bad multicount <%s>\n", cp);
			goto Usage;
		}
		break;
	case 'W':
		++Wflag;
		break;
	case 'w':
		++wflag;
		break;
	case 'v':
		++vflag;
		break;
        case 't':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		cp = *++av;
		nt = strtoll(cp, &ep, 0);
		if (cp == ep || *ep) {
			std::cerr << "Can't parse thread count <" << cp << ">" << std::endl;
			goto Usage;
		}
		break;
	case '-':
		break;
	default:
		std::cerr << "bad flag " << *ap << std::endl;
	Usage:
		std::cerr << "Usage: lb3 -b bucket [-wv] [-s size] [-e endpoint]" << std::endl;
		exit(1);
	}
	if (nt <= 0) {
		std::cerr << "Bad thread count " << nt << std::endl;
		goto Usage;
	}
	if (!my_container) {
		std::cerr << "-b XXX required " << nt << std::endl;
		goto Usage;
	}
	if (fixed_size) {
		fixed_size_buf = (char*)malloc(fixed_size);
		if (!fixed_size_buf) {
			std::cerr << "Can't allocate fixed_size buf of size " << fixed_size << std::endl;
			goto Usage;
		}
		memset(fixed_size_buf, 0, fixed_size);
	}
	{
		Aws::SDKOptions options;
		Aws::InitAPI(options);
		process();
		Aws::ShutdownAPI(options);
	}
	exit(exitcode);
}
